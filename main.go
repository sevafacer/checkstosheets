package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// --- Конфигурация ---
const (
	maxRetries         = 3
	retryDelay         = 2
	tokenRefreshWindow = 5 * time.Minute
	maxGoroutines      = 100 // *** Увеличено для worker pool ***
	numWorkers         = 50  // *** Количество worker-ов в пуле ***
	sheetIDRange       = "'Чеки'!B:B"
	sheetUpdateRange   = "'Чеки'!B%d:G%d"
	sheetFormatRange   = "'Чеки'!B%d:G%d"
	sheetIDPropID      = 1051413829
	mediaGroupCacheTTL = 1 * time.Minute
)

var fieldKeywords = map[string][]string{
	"address": {"адрес", "объект", "квартира", "школа", "дом", "улица", "место", "локация"},
	"amount":  {"сумма", "стоимость", "оплата", "платёж", "цена"},
	"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение", "заметка"},
}

var (
	oauthConfig *oauth2.Config
	oauthState  = "state-token"
	authCodeCh  = make(chan string)
	wg          sync.WaitGroup
	semaphore   = make(chan struct{}, maxGoroutines)

	mediaGroupCache    = make(map[string]*MediaGroupData)
	mediaGroupCacheMu  sync.Mutex
	mediaGroupExpiryCh = make(chan string, 100)

	taskQueue   = make(chan FileTask, 100)   // *** Канал для задач worker pool ***
	resultsChan = make(chan FileResult, 100) // *** Канал для результатов worker pool ***
)

type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string
}

type TokenInfo struct {
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type"`
	RefreshToken string    `json:"refresh_token"`
	Expiry       time.Time `json:"expiry"`
}

type MediaGroupData struct {
	Files       map[string]bool
	Caption     string
	Address     string
	Amount      string
	Comment     string
	LastUpdated time.Time
	UserID      int64
	ChatID      int64
	Username    string
}

// ---  Worker Pool Structures ---

type FileTask struct {
	FileID         string
	FileURL        string
	BaseName       string
	DateFormatted  string
	Amount         string
	DriveService   *drive.Service
	ObjectFolderID string
}

type FileResult struct {
	FileID    string
	DriveLink string
	Error     error
}

// --- Функции для загрузки конфигурации из переменных окружения ---
// ... (loadEnvVars, getOAuthClient, startOAuthServer, saveTokenToEnv, loadTokenFromEnv, refreshToken - без изменений) ...
func loadEnvVars() (telegramToken string, spreadsheetId string, driveFolderId string, adminID int64, googleClientID string, googleClientSecret string, webhookURL string) {
	telegramToken = os.Getenv("TELEGRAM_BOT_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN не установлен в переменных окружения")
	}

	spreadsheetId = os.Getenv("GOOGLE_SHEET_ID")
	if spreadsheetId == "" {
		log.Fatal("GOOGLE_SHEET_ID не установлен в переменных окружения")
	}

	driveFolderId = os.Getenv("GOOGLE_DRIVE_FOLDER_ID")
	if driveFolderId == "" {
		log.Fatal("GOOGLE_DRIVE_FOLDER_ID не установлен в переменных окружения")
	}

	adminIDStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	if adminIDStr == "" {
		log.Fatal("ADMIN_CHAT_ID не установлен в переменных окружения")
	}
	var err error
	adminID, err = strconv.ParseInt(adminIDStr, 10, 64)
	if err != nil {
		log.Fatalf("Неверный формат ADMIN_CHAT_ID: %v", err)
	}

	googleClientID = os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret = os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	if googleClientID == "" || googleClientSecret == "" {
		log.Fatal("GOOGLE_OAUTH_CLIENT_ID и GOOGLE_OAUTH_CLIENT_SECRET должны быть установлены в переменных окружения")
	}

	webhookURL = os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		log.Fatal("WEBHOOK_URL не установлен в переменных окружения")
	}
	return
}

func getOAuthClient(config *oauth2.Config) (*http.Client, error) {
	token, err := loadTokenFromEnv()
	if err == nil {
		if time.Until(token.Expiry) < tokenRefreshWindow {
			if token.RefreshToken != "" {
				newToken, err := refreshToken(config, token)
				if err == nil {
					if err := saveTokenToEnv(newToken); err != nil {
						log.Printf("Предупреждение: не удалось сохранить обновлённый токен: %v", err)
					}
					return config.Client(context.Background(), newToken), nil
				}
				log.Printf("Предупреждение: не удалось обновить токен: %v", err)
			}
		} else if token.Valid() {
			return config.Client(context.Background(), token), nil
		}
	}

	serverErrCh := make(chan error, 1)
	server := startOAuthServer(serverErrCh)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Ошибка при остановке OAuth сервера: %v", err)
		}
	}()

	authURL := config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	fmt.Printf("Перейдите по ссылке для авторизации:\n%v\n", authURL)

	select {
	case code := <-authCodeCh:
		token, err := config.Exchange(context.Background(), code)
		if err != nil {
			return nil, fmt.Errorf("ошибка обмена кода на токен: %v", err)
		}

		if err := saveTokenToEnv(token); err != nil {
			log.Printf("Предупреждение: не удалось сохранить новый токен: %v", err)
		}

		return config.Client(context.Background(), token), nil

	case err := <-serverErrCh:
		return nil, fmt.Errorf("ошибка OAuth сервера: %v", err)

	case <-time.After(5 * time.Minute):
		return nil, fmt.Errorf("превышено время ожидания авторизации")
	}
}

func startOAuthServer(errCh chan<- error) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != oauthState {
			http.Error(w, "Неверный state", http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "Код не найден в запросе", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "Авторизация прошла успешно. Вы можете закрыть это окно.")
		authCodeCh <- code
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Запуск OAuth2 сервера на https://railway.app/")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	return server
}

func saveTokenToEnv(token *oauth2.Token) error {
	if token == nil {
		return fmt.Errorf("попытка сохранить пустой токен")
	}

	tokenInfo := TokenInfo{
		AccessToken:  token.AccessToken,
		TokenType:    token.TokenType,
		RefreshToken: token.RefreshToken,
		Expiry:       token.Expiry,
	}

	if token.RefreshToken == "" && os.Getenv("GOOGLE_OAUTH_TOKEN") != "" {
		oldToken, err := loadTokenFromEnv()
		if err == nil && oldToken.RefreshToken != "" {
			tokenInfo.RefreshToken = oldToken.RefreshToken
		}
	}

	tokenJSON, err := json.Marshal(tokenInfo)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга токена: %v", err)
	}

	encodedToken := base64.StdEncoding.EncodeToString(tokenJSON)
	return os.Setenv("GOOGLE_OAUTH_TOKEN", encodedToken)
}

func loadTokenFromEnv() (*oauth2.Token, error) {
	tokenStr := os.Getenv("GOOGLE_OAUTH_TOKEN")
	if tokenStr == "" {
		return nil, fmt.Errorf("токен не найден в переменных окружения")
	}

	tokenJSON, err := base64.StdEncoding.DecodeString(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("ошибка декодирования токена: %v", err)
	}

	var tokenInfo TokenInfo
	if err := json.Unmarshal(tokenJSON, &tokenInfo); err != nil {
		return nil, fmt.Errorf("ошибка анмаршалинга токена: %v", err)
	}

	token := &oauth2.Token{
		AccessToken:  tokenInfo.AccessToken,
		TokenType:    tokenInfo.TokenType,
		RefreshToken: tokenInfo.RefreshToken,
		Expiry:       tokenInfo.Expiry,
	}

	return token, nil
}

func refreshToken(config *oauth2.Config, token *oauth2.Token) (*oauth2.Token, error) {
	for i := 0; i < maxRetries; i++ {
		tokenSource := config.TokenSource(context.Background(), token)
		newToken, err := tokenSource.Token()
		if err == nil {
			if newToken.RefreshToken == "" {
				newToken.RefreshToken = token.RefreshToken
			}
			return newToken, nil
		}

		log.Printf("Попытка %d обновления токена не удалась: %v", i+1, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return nil, fmt.Errorf("не удалось обновить токен после %d попыток", maxRetries)
}

// --- Функции для работы с Google Drive ---
// ... (ensureObjectFolder, uploadFileToDrive, refreshDriveService - без изменений) ...
func ensureObjectFolder(service *drive.Service, parentFolderId, objectName string) (string, error) {
	sanitizedObjectName := strings.TrimSpace(objectName)

	if sanitizedObjectName == "" {
		sanitizedObjectName = "Разное"
	}

	sanitizedObjectName = sanitizeFileName(sanitizedObjectName)

	query := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and trashed = false",
		sanitizedObjectName, parentFolderId)

	var fileList *drive.FileList
	var err error

	for i := 0; i < maxRetries; i++ {
		fileList, err = service.Files.List().
			Q(query).
			Fields("files(id, name)").
			PageSize(10).
			Do()

		if err == nil {
			break
		}

		log.Printf("Попытка %d поиска папки объекта не удалась: %v", i+1, err)
		service, err = refreshDriveService(service, err) // Используем функцию обновления сервиса
		if err != nil {
			continue // Если не удалось обновить сервис, переходим к следующей попытке
		}
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}

	if err != nil {
		return "", fmt.Errorf("не удалось выполнить поиск папки после %d попыток: %v", maxRetries, err)
	}

	if len(fileList.Files) > 0 {
		log.Printf("Найдена существующая папка объекта '%s' (ID: %s)", sanitizedObjectName, fileList.Files[0].Id)
		return fileList.Files[0].Id, nil
	}

	log.Printf("Создаём новую папку объекта '%s'", sanitizedObjectName)

	folder := &drive.File{
		Name:     sanitizedObjectName,
		Parents:  []string{parentFolderId},
		MimeType: "application/vnd.google-apps.folder",
	}

	var createdFolder *drive.File

	for i := 0; i < maxRetries; i++ {
		createdFolder, err = service.Files.Create(folder).Fields("id, name").Do()
		if err == nil {
			break
		}

		log.Printf("Попытка %d создания папки объекта не удалась: %v", i+1, err)
		service, err = refreshDriveService(service, err) // Используем функцию обновления сервиса
		if err != nil {
			continue // Если не удалось обновить сервис, переходим к следующей попытке
		}
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}

	if err != nil {
		return "", fmt.Errorf("не удалось создать папку объекта после %d попыток: %v", maxRetries, err)
	}

	log.Printf("Успешно создана папка объекта '%s' (ID: %s)", sanitizedObjectName, createdFolder.Id)
	return createdFolder.Id, nil
}

func uploadFileToDrive(service *drive.Service, filePath, fileName, folderId string) (string, error) {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		f, err := os.Open(filePath)
		if err != nil {
			return "", fmt.Errorf("не удалось открыть файл: %v", err)
		}
		defer f.Close()

		file := &drive.File{
			Name:    fileName,
			Parents: []string{folderId},
		}

		res, err := service.Files.Create(file).Media(f).Fields("webViewLink").Do()
		if err == nil {
			return res.WebViewLink, nil
		}

		lastErr = err
		log.Printf("Попытка %d загрузки файла не удалась: %v", i+1, err)

		service, err = refreshDriveService(service, err) // Используем функцию обновления сервиса
		if err != nil {
			continue // Если не удалось обновить сервис, переходим к следующей попытке
		}

		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}

	return "", fmt.Errorf("не удалось загрузить файл после %d попыток: %v", maxRetries, lastErr)
}

// refreshDriveService обрабатывает ошибку "token expired" и обновляет сервис Drive.
func refreshDriveService(service *drive.Service, originalErr error) (*drive.Service, error) {
	if strings.Contains(originalErr.Error(), "oauth2: token expired") {
		newClient, err := getOAuthClient(oauthConfig)
		if err != nil {
			log.Printf("Не удалось обновить клиент OAuth: %v", err)
			return service, fmt.Errorf("не удалось обновить клиент OAuth: %v", err) // Возвращаем ошибку
		}

		newService, err := drive.NewService(context.Background(), option.WithHTTPClient(newClient))
		if err != nil {
			log.Printf("Не удалось создать новый Drive сервис: %v", err)
			return service, fmt.Errorf("не удалось создать новый Drive сервис: %v", err) // Возвращаем ошибку
		}
		return newService, nil // Возвращаем обновленный сервис
	}
	return service, originalErr // Возвращаем оригинальный сервис и ошибку, если ошибка не связана с токеном
}

// --- Функции для работы с Google Sheets ---
// ... (appendToSheet - без изменений) ...
func appendToSheet(service *sheets.Service, spreadsheetId string, data ParsedData) error {
	values := []interface{}{
		data.Date,
		data.Username, data.Address,
		data.Amount,
		data.Comment,
		data.DriveLink,
	}

	vr := &sheets.ValueRange{
		Values: [][]interface{}{values},
	}

	resp, err := service.Spreadsheets.Values.Get(spreadsheetId, sheetIDRange).Do()
	if err != nil {
		return fmt.Errorf("не удалось получить данные из Google Sheets: %v", err)
	}

	lastRow := 1
	if len(resp.Values) > 0 {
		lastRow = len(resp.Values) + 1
	}

	rangeStr := fmt.Sprintf(sheetUpdateRange, lastRow, lastRow)

	_, err = service.Spreadsheets.Values.Update(spreadsheetId, rangeStr, vr).
		ValueInputOption("USER_ENTERED").
		Do()
	if err != nil {
		return fmt.Errorf("не удалось обновить Google Sheets: %v", err)
	}

	formatRequest := sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{
			{
				RepeatCell: &sheets.RepeatCellRequest{
					Range: &sheets.GridRange{
						SheetId:          sheetIDPropID,
						StartRowIndex:    int64(lastRow - 1),
						EndRowIndex:      int64(lastRow),
						StartColumnIndex: 3,
						EndColumnIndex:   4,
					},
					Cell: &sheets.CellData{
						UserEnteredFormat: &sheets.CellFormat{
							NumberFormat: &sheets.NumberFormat{
								Type:    "NUMBER",
								Pattern: "#,##0.00[$₽]",
							},
						},
					},
					Fields: "userEnteredFormat.numberFormat",
				},
			},
		},
	}

	_, err = service.Spreadsheets.BatchUpdate(spreadsheetId, &formatRequest).Do()
	if err != nil {
		log.Printf("Предупреждение: не удалось установить форматирование: %v", err)
	}

	return nil
}

// --- Функции Telegram Bot ---
// ... (keepAlive, startMediaGroupCacheCleaner - без изменений) ...
func keepAlive(webhookURL string) {
	ticker := time.NewTicker(10 * time.Minute)
	go func() {
		for range ticker.C {
			resp, err := http.Get(webhookURL)
			if err != nil {
				log.Printf("Ошибка при выполнении keepalive запроса: %v", err)
				continue
			}
			resp.Body.Close()
		}
	}()
}

func startMediaGroupCacheCleaner() {
	go func() {
		for mediaGroupID := range mediaGroupExpiryCh {
			mediaGroupCacheMu.Lock()
			delete(mediaGroupCache, mediaGroupID)
			mediaGroupCacheMu.Unlock()
			log.Printf("Медиагруппа %s удалена из кэша", mediaGroupID)
		}
	}()
}

// --- Worker Pool ---

func startWorkers(numWorkers int, driveService *drive.Service, objectFolderID string) {
	for i := 0; i < numWorkers; i++ {
		go worker(taskQueue, resultsChan, driveService, objectFolderID)
	}
}

func worker(taskQueue <-chan FileTask, resultsChan chan<- FileResult, driveService *drive.Service, objectFolderID string) {
	for task := range taskQueue {
		link, err := downloadAndUploadFile(task.FileURL, task.BaseName, task.DateFormatted, task.Amount, driveService, objectFolderID)
		resultsChan <- FileResult{FileID: task.FileID, DriveLink: link, Error: err}
	}
}

// --- Обработчики сообщений Telegram ---

func handleMediaGroupMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	if len(message.Photo) == 0 {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Сообщение не содержит фотографии. Попробуйте снова.")
		bot.Send(reply)
		return
	}

	// Получаем фото с лучшим качеством (последнее в массиве)
	bestPhoto := message.Photo[len(message.Photo)-1]

	// Определяем часовой пояс Москвы для формирования имени файла
	moscowOffset := int((3 * time.Hour).Seconds())
	moscowTime := time.Unix(int64(message.Date), 0).UTC().Add(time.Duration(moscowOffset) * time.Second)
	dateFormatted := moscowTime.Format("02.01.2006")

	// Обработка медиагруппы
	if message.MediaGroupID != "" {
		mediaGroupCacheMu.Lock()

		// Инициализируем группу, если она еще не существует
		if _, exists := mediaGroupCache[message.MediaGroupID]; !exists {
			caption := message.Caption

			// Парсим данные только если есть подпись
			var address, amount, commentText string
			if caption != "" {
				var parseErr error
				address, amount, commentText, parseErr = parseMessage(caption)
				if parseErr != nil {
					mediaGroupCacheMu.Unlock()
					reply := tgbotapi.NewMessage(message.Chat.ID, "Не удалось распознать подпись к фото. Проверьте формат данных.")
					bot.Send(reply)
					return
				}
			}

			mediaGroupCache[message.MediaGroupID] = &MediaGroupData{
				Files:       make(map[string]bool),
				Caption:     caption,
				Address:     address,
				Amount:      amount,
				Comment:     commentText,
				LastUpdated: time.Now(),
				UserID:      message.From.ID,
				ChatID:      message.Chat.ID,
				Username:    getFullName(message.From),
			}

			// Запускаем таймер для очистки кэша
			go func(mediaGroupID string) {
				time.Sleep(mediaGroupCacheTTL) // Используем константу TTL
				mediaGroupExpiryCh <- mediaGroupID
			}(message.MediaGroupID)
		}

		// Добавляем файл в группу, если его еще нет
		if _, processed := mediaGroupCache[message.MediaGroupID].Files[bestPhoto.FileID]; !processed {
			mediaGroupCache[message.MediaGroupID].Files[bestPhoto.FileID] = false
			mediaGroupCache[message.MediaGroupID].LastUpdated = time.Now()
		}

		mediaGroupCacheMu.Unlock()

		// Обрабатываем группу, если пришло фото с подписью или это первое фото группы
		if message.Caption != "" || len(mediaGroupCache[message.MediaGroupID].Files) == 1 {
			go processMediaGroup(bot, message.MediaGroupID, sheetsService, spreadsheetId, driveService, parentFolderId, adminID)
		}

		return
	}

	// Обработка одиночного фото (без изменений)
	caption := message.Caption
	address, amount, commentText, parseErr := parseMessage(caption)
	if parseErr != nil {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Не удалось распознать подпись к фото. Проверьте формат данных.")
		bot.Send(reply)
		return
	}

	// ===  Добавляем проверку на заполненность адреса и суммы ===
	if address == "" {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Пожалуйста, введите адрес объекта.")
		bot.Send(reply)
		return
	}
	if amount == "" {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Пожалуйста, введите сумму.")
		bot.Send(reply)
		return
	}
	// === Конец проверки ===

	// Создаем или получаем ID папки объекта на Google Drive
	objectFolderID, err := ensureObjectFolder(driveService, parentFolderId, address)
	if err != nil {
		log.Printf("Ошибка создания папки для объекта: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Ошибка обработки объекта. Попробуйте позже.")
		bot.Send(reply)
		return
	}

	sanitizedAddress := sanitizeFileName(address)
	photoFile, err := bot.GetFile(tgbotapi.FileConfig{FileID: bestPhoto.FileID})
	if err != nil {
		log.Printf("Ошибка загрузки файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Ошибка загрузки файла. Попробуйте позже.")
		bot.Send(reply)
		return
	}

	fileURL := photoFile.Link(bot.Token)
	link, err := downloadAndUploadFile(fileURL, sanitizedAddress, dateFormatted, amount, driveService, objectFolderID)
	if err != nil {
		log.Printf("Ошибка при обработке файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Ошибка загрузки файла на Google Drive. Попробуйте позже.")
		bot.Send(reply)
		return
	}

	parsedData := ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  getFullName(message.From),
		Date:      time.Now().Format("02/01/2006 15:04:05"),
		DriveLink: link,
	}

	if err := appendToSheet(sheetsService, spreadsheetId, parsedData); err != nil {
		log.Printf("Ошибка записи в Google Sheets: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Ошибка записи данных. Попробуйте позже.")
		bot.Send(reply)
		return
	}

	reply := tgbotapi.NewMessage(message.Chat.ID, "Фотография успешно обработана.")
	bot.Send(reply)
}

func processMediaGroup(bot *tgbotapi.BotAPI, mediaGroupID string, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	time.Sleep(1 * time.Second)

	mediaGroupCacheMu.Lock()
	groupData, exists := mediaGroupCache[mediaGroupID]
	if !exists {
		mediaGroupCacheMu.Unlock()
		log.Printf("Ошибка: медиагруппа %s не найдена в кэше", mediaGroupID)
		return
	}

	hasUnprocessedFiles := false
	for _, processed := range groupData.Files {
		if !processed {
			hasUnprocessedFiles = true
			break
		}
	}

	if !hasUnprocessedFiles || len(groupData.Files) == 0 {
		mediaGroupCacheMu.Unlock()
		return
	}

	address := groupData.Address
	amount := groupData.Amount
	commentText := groupData.Comment
	chatID := groupData.ChatID
	username := groupData.Username

	// === Добавляем проверку на заполненность адреса и суммы ===
	if address == "" {
		reply := tgbotapi.NewMessage(chatID, "Пожалуйста, введите адрес объекта.")
		bot.Send(reply)
		mediaGroupCacheMu.Unlock() // Важно разблокировать мьютекс перед выходом
		return
	}
	if amount == "" {
		reply := tgbotapi.NewMessage(chatID, "Пожалуйста, введите сумму.")
		bot.Send(reply)
		mediaGroupCacheMu.Unlock() // Важно разблокировать мьютекс перед выходом
		return
	}
	// === Конец проверки ===

	unprocessedFileIDs := make([]string, 0)
	for fileID, processed := range groupData.Files {
		if !processed {
			unprocessedFileIDs = append(unprocessedFileIDs, fileID)
		}
	}
	mediaGroupCacheMu.Unlock()

	if len(unprocessedFileIDs) == 0 {
		return
	}

	objectFolderID, err := ensureObjectFolder(driveService, parentFolderId, address)
	if err != nil {
		log.Printf("Ошибка создания папки для объекта: %v", err)
		reply := tgbotapi.NewMessage(chatID, "Ошибка обработки объекта. Попробуйте позже.")
		bot.Send(reply)
		return
	}

	var tasks []FileTask
	moscowOffset := int((3 * time.Hour).Seconds())
	moscowTime := time.Now().UTC().Add(time.Duration(moscowOffset) * time.Second)
	dateFormatted := moscowTime.Format("02.01.2006")
	sanitizedAddress := sanitizeFileName(address)

	for i, fileID := range unprocessedFileIDs {
		photoFile, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
		if err != nil {
			log.Printf("Ошибка загрузки файла из медиагруппы (ID: %s): %v", fileID, err)
			continue // Пропускаем файл с ошибкой, но продолжаем обработку остальных
		}
		fileURL := photoFile.Link(bot.Token)
		indexedFileName := fmt.Sprintf("%s_%d", sanitizedAddress, i+1)

		tasks = append(tasks, FileTask{
			FileID:         fileID,
			FileURL:        fileURL,
			BaseName:       indexedFileName,
			DateFormatted:  dateFormatted,
			Amount:         amount,
			DriveService:   driveService,
			ObjectFolderID: objectFolderID,
		})
	}

	// Запускаем воркеры для обработки файлов
	startWorkers(numWorkers, driveService, objectFolderID)

	// Отправляем задачи в очередь
	go func() {
		for _, task := range tasks {
			taskQueue <- task
		}
		close(taskQueue) // Закрываем taskQueue после отправки всех задач
	}()

	// Собираем результаты обработки файлов
	var links []string
	successCount := 0
	processedFiles := make(map[string]bool) // Для отслеживания обработанных FileID

	for range tasks { // Ожидаем результатов для каждого файла в tasks
		result := <-resultsChan
		if result.Error != nil {
			log.Printf("Ошибка обработки файла из worker pool, FileID: %s, ошибка: %v", result.FileID, result.Error)
		} else {
			links = append(links, result.DriveLink)
			successCount++
			processedFiles[result.FileID] = true // Помечаем FileID как обработанный
		}
	}
	close(resultsChan) // Закрываем resultsChan после сбора всех результатов

	// Обновляем кэш медиагруппы, помечая успешно обработанные файлы
	mediaGroupCacheMu.Lock()
	if group, exists := mediaGroupCache[mediaGroupID]; exists {
		for fileID := range processedFiles {
			group.Files[fileID] = true // Помечаем только успешно обработанные
		}
	}
	mediaGroupCacheMu.Unlock()

	if len(links) == 0 {
		reply := tgbotapi.NewMessage(chatID, "Не удалось загрузить ни одной фотографии из медиагруппы. Попробуйте снова.")
		bot.Send(reply)
		return
	}

	parsedData := ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  username,
		Date:      time.Now().Format("02/01/2006 15:04:05"),
		DriveLink: strings.Join(links, " "),
	}

	// *** Асинхронная запись в Google Sheets ***
	go func() {
		if err := appendToSheet(sheetsService, spreadsheetId, parsedData); err != nil {
			log.Printf("Ошибка асинхронной записи в Google Sheets: %v", err) // Логируем ошибку асинхронной записи
			notifyAdminAboutSheetError(bot, adminID, err, mediaGroupID)      // Оповещаем админа об ошибке
		} else {
			log.Printf("Успешная асинхронная запись в Google Sheets для медиагруппы: %s", mediaGroupID) // Логируем успешную запись
		}
	}()

	successMessage := fmt.Sprintf("Успешно обработано %d фото из медиагруппы.", successCount)
	reply := tgbotapi.NewMessage(chatID, successMessage)
	bot.Send(reply)
}

func notifyAdminAboutSheetError(bot *tgbotapi.BotAPI, adminID int64, err error, mediaGroupID string) {
	errorMessage := fmt.Sprintf("Ошибка записи в Google Sheets для медиагруппы %s: %v", mediaGroupID, err)
	adminMessage := tgbotapi.NewMessage(adminID, errorMessage)
	_, sendErr := bot.Send(adminMessage)
	if sendErr != nil {
		log.Printf("Не удалось отправить сообщение администратору об ошибке Google Sheets: %v", sendErr)
	}
}

func downloadAndUploadFile(fileURL, baseName, dateFormatted, amount string, driveService *drive.Service, objectFolderID string) (string, error) {
	log.Printf("Начало скачивания файла из URL: %s", fileURL) // Лог начала скачивания
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("ошибка скачивания файла: %v", err)
	}
	defer resp.Body.Close()

	fileName := sanitizeFileName(fmt.Sprintf("%s_%s_%s.jpg", baseName, dateFormatted, amount))
	tmpFile, err := os.CreateTemp("", fileName)
	if err != nil {
		return "", fmt.Errorf("ошибка создания временного файла: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	log.Printf("Начало копирования скачанного файла во временный файл: %s", tmpFile.Name()) // Лог начала копирования
	if _, err = io.Copy(tmpFile, resp.Body); err != nil {
		return "", fmt.Errorf("ошибка копирования файла: %v", err)
	}
	tmpFile.Close()
	log.Printf("Копирование файла во временный файл завершено: %s", tmpFile.Name()) // Лог завершения копирования

	log.Printf("Начало загрузки файла на Google Drive: %s, имя файла: %s, папка ID: %s", tmpFile.Name(), fileName, objectFolderID) // Лог начала загрузки
	link, err := uploadFileToDrive(driveService, tmpFile.Name(), fileName, objectFolderID)
	if err != nil {
		return "", fmt.Errorf("ошибка загрузки файла на Google Drive: %v", err)
	}
	log.Printf("Загрузка файла на Google Drive завершена, ссылка: %s", link) // Лог завершения загрузки

	return link, nil
}

// --- Функции парсинга ---
// ... (getFullName, fieldMatch, removeLeadingKeyword, fallbackParse, parseMessage, cleanAmount, sanitizeFileName - без изменений) ...
func getFullName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

type fieldMatch struct {
	field string
	start int
	end   int
}

func removeLeadingKeyword(text string, keywords []string) string {
	trimmed := strings.TrimSpace(text)
	lower := strings.ToLower(trimmed)
	for _, kw := range keywords {
		if strings.HasPrefix(lower, kw) {
			// Удаляем ключевое слово и последующие пробелы.
			without := strings.TrimSpace(trimmed[len(kw):])
			return without
		}
	}
	return trimmed
}

func fallbackParse(message string) (address string, amount string, comment string, err error) {
	// Если сообщение многострочное, обрабатываем каждую строку отдельно.
	if strings.Contains(message, "\n") {
		lines := strings.Split(message, "\n")
		if len(lines) > 0 {
			address = removeLeadingKeyword(strings.TrimSpace(lines[0]), fieldKeywords["address"])
		}
		if len(lines) > 1 {
			amount = removeLeadingKeyword(strings.TrimSpace(lines[1]), fieldKeywords["amount"])
			amount = cleanAmount(amount)
		}
		if len(lines) > 2 {
			comment = removeLeadingKeyword(strings.TrimSpace(strings.Join(lines[2:], "\n")), fieldKeywords["comment"])
		}
	} else {
		lowerMsg := strings.ToLower(message)
		amountIdx := -1
		for _, kw := range fieldKeywords["amount"] {
			idx := strings.Index(lowerMsg, kw)
			if idx != -1 && (amountIdx == -1 || idx < amountIdx) {
				amountIdx = idx
			}
		}
		if amountIdx != -1 {
			addressPart := strings.TrimSpace(message[:amountIdx])
			amountPart := strings.TrimSpace(message[amountIdx:])
			address = removeLeadingKeyword(addressPart, fieldKeywords["address"])
			amountPart = removeLeadingKeyword(amountPart, fieldKeywords["amount"])
			commentIdx := -1
			lowerAmountPart := strings.ToLower(amountPart)
			for _, kw := range fieldKeywords["comment"] {
				idx := strings.Index(lowerAmountPart, kw)
				if idx != -1 && (commentIdx == -1 || idx < commentIdx) {
					commentIdx = idx
				}
			}
			if commentIdx != -1 {
				amt := strings.TrimSpace(amountPart[:commentIdx])
				comment = removeLeadingKeyword(strings.TrimSpace(amountPart[commentIdx:]), fieldKeywords["comment"])
				amount = cleanAmount(amt)
			} else {
				amount = cleanAmount(amountPart)
			}
		} else {
			// Если нет ключевого слова суммы – считаем всё как адрес.
			address = removeLeadingKeyword(message, fieldKeywords["address"])
		}
	}

	if address == "" || amount == "" {
		return "", "", "", errors.New("не удалось найти обязательные поля: адрес и сумма")
	}
	return address, amount, comment, nil
}

func parseMessage(message string) (address string, amount string, comment string, err error) {
	if strings.TrimSpace(message) == "" {
		return "", "", "", errors.New("пустое сообщение")
	}

	// Если сообщение содержит символы ":" или "=" – пробуем regex-подход.
	if strings.Contains(message, ":") || strings.Contains(message, "=") {
		normalized := strings.Join(strings.Fields(message), " ")
		var matches []fieldMatch
		for field, keywords := range fieldKeywords {
			for _, kw := range keywords {
				// Шаблон: ключевое слово + разделитель с пробелами.
				pattern := fmt.Sprintf("(?i)%s\\s*[:=]\\s*", regexp.QuoteMeta(kw))
				re := regexp.MustCompile(pattern)
				locs := re.FindAllStringIndex(normalized, -1)
				for _, loc := range locs {
					matches = append(matches, fieldMatch{
						field: field,
						start: loc[0],
						end:   loc[1],
					})
				}
			}
		}

		if len(matches) > 0 {
			sort.Slice(matches, func(i, j int) bool {
				return matches[i].start < matches[j].start
			})
			fieldValues := make(map[string]string)
			for i, m := range matches {
				endPos := len(normalized)
				if i < len(matches)-1 {
					endPos = matches[i+1].start
				}
				value := strings.TrimSpace(normalized[m.end:endPos])
				if _, exists := fieldValues[m.field]; !exists && value != "" {
					fieldValues[m.field] = value
				}
			}
			address = fieldValues["address"]
			amount = cleanAmount(fieldValues["amount"])
			comment = fieldValues["comment"]
		}

		// Если обязательные поля не получены – переходим в fallback.
		if address == "" || amount == "" {
			return fallbackParse(message)
		}
		return address, amount, comment, nil
	}

	// Если разделителей нет – сразу используем fallback.
	return fallbackParse(message)
}

func cleanAmount(amount string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	cleaned := re.ReplaceAllString(amount, "")
	cleaned = strings.ReplaceAll(cleaned, ".", ",")
	return cleaned
}

func sanitizeFileName(name string) string {
	re := regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s\.-]`)
	sanitized := re.ReplaceAllString(name, "_")

	multipleSpaces := regexp.MustCompile(`\s+`)
	sanitized = multipleSpaces.ReplaceAllString(sanitized, " ")

	sanitized = strings.ReplaceAll(sanitized, " ", "_")

	multipleUnderscore := regexp.MustCompile(`_+`)
	sanitized = multipleUnderscore.ReplaceAllString(sanitized, "_")

	sanitized = strings.Trim(sanitized, "_")

	return sanitized
}

// --- Основная функция main ---
// ... (main - с изменениями для worker pool и увеличенным maxGoroutines) ...
func main() {
	// --- 1. Загрузка переменных окружения ---
	telegramToken, spreadsheetId, driveFolderId, adminID, googleClientID, googleClientSecret, webhookURL := loadEnvVars()

	// --- 2. Настройка OAuth2 конфигурации Google ---
	oauthConfig = &oauth2.Config{
		ClientID:     googleClientID,
		ClientSecret: googleClientSecret,
		RedirectURL:  "https://checkstosheets-production.up.railway.app/",
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive.file",
		},
		Endpoint: google.Endpoint,
	}

	// --- 3. Получение HTTP клиента Google API ---
	client, err := getOAuthClient(oauthConfig)
	if err != nil {
		log.Fatalf("Не удалось получить OAuth2 клиента: %v", err)
	}

	// --- 4. Создание сервисов Google Sheets и Drive ---
	sheetsService, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Не удалось создать Sheets сервис: %v", err)
	}
	driveService, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Не удалось создать Drive сервис: %v", err)
	}

	// --- 5. Инициализация Telegram Bot API ---
	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Panic(err)
	}
	bot.Debug = true
	log.Printf("Авторизовался как %s", bot.Self.UserName)

	// --- 6. Настройка Webhook для Telegram ---
	parsedWebhookURL, err := url.Parse(webhookURL)
	if err != nil {
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}

	webhookConfig := tgbotapi.WebhookConfig{
		URL:            parsedWebhookURL,
		MaxConnections: 40,
	}
	_, err = bot.Request(webhookConfig)
	if err != nil {
		log.Fatalf("Не удалось установить Webhook: %v", err)
	}
	log.Printf("Webhook установлен на %s", webhookURL)
	keepAlive(webhookURL)

	// --- 7. Запуск очистителя кэша медиагрупп ---
	startMediaGroupCacheCleaner()

	// *** 8. Запуск Worker Pool для обработки файлов медиагрупп ***
	driveFolderID := os.Getenv("GOOGLE_DRIVE_FOLDER_ID")  // Получаем folderID здесь, чтобы передать в воркеры
	startWorkers(numWorkers, driveService, driveFolderID) // Запускаем воркеры

	// --- 9. Обработка HTTP запросов (Webhook от Telegram) ---
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			bytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("Ошибка чтения тела запроса: %v", err)
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}

			var update tgbotapi.Update
			err = json.Unmarshal(bytes, &update)
			if err != nil {
				log.Printf("Ошибка декодирования обновления: %v", err)
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}

			if update.Message != nil {
				if update.Message.IsCommand() {
					switch update.Message.Command() {
					case "start", "help":
						helpText := `Привет! Я бот позволяющий грамотно отслеживать все чеки. Вот как меня использовать:

1. Прикрепите фотографию чека (Четкую и качественную)
2. В подписи к фото укажите:
   Адрес: [адрес]
   Сумма: [сумма]
   Комментарий: [ваш комментарий] (необязательно)

     или

   Адрес [адрес]
   Сумма [сумма]
   Комментарий [ваш комментарий] (необязательно)

Примеры:
Адрес: ул. Пушкина, д. 10
Сумма: 1500 руб
Комментарий: Оплата за сентябрь
или
Адрес ул. Пушкина, д. 10
Сумма 1500 руб
Комментарий Оплата за сентябрь`

						msg := tgbotapi.NewMessage(update.Message.Chat.ID, helpText)
						bot.Send(msg)
					}
				} else if update.Message.Photo != nil || update.Message.MediaGroupID != "" {
					semaphore <- struct{}{}
					wg.Add(1)
					go func(message *tgbotapi.Message) {
						defer wg.Done()
						handleMediaGroupMessage(bot, message, sheetsService, spreadsheetId, driveService, driveFolderId, adminID)
						<-semaphore
					}(update.Message)
				}
			}
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
		}
	})

	// --- 10. Запуск HTTP сервера ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Запуск HTTP сервера на порту %s", port)
	server := &http.Server{Addr: ":" + port}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска HTTP сервера: %v", err)
		}
	}()

	// --- 11. Graceful Shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Println("Получен сигнал завершения, останавливаем сервер...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Ошибка при остановке HTTP-сервера: %v", err)
	}
	wg.Wait()
	log.Println("Сервер успешно остановлен.")
}
