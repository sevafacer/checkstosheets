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

// ========================
// Константы и глобальные переменные 🚀
// ========================
const (
	maxRetries           = 3
	retryDelay           = 2 // базовая задержка между повторами (сек)
	tokenRefreshWindow   = 5 * time.Minute
	mediaGroupCacheTTL   = 3 * time.Minute
	numWorkers           = 50
	sheetIDRange         = "'Чеки'!B:B"
	sheetUpdateRange     = "'Чеки'!B%d:G%d"
	sheetFormatRange     = "'Чеки'!B%d:G%d"
	sheetIDPropID        = 1051413829
	maxConcurrentUploads = 5
)

var (
	// Ключевые слова для распознавания полей из подписи (адрес, сумма, комментарий)
	fieldKeywords = map[string][]string{
		"address": {"адрес", "объект", "квартира", "школа", "дом", "улица", "место", "локация"},
		"amount":  {"сумма", "стоимость", "оплата", "платёж", "цена"},
		"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение", "заметка"},
	}

	// Мьютексы и каналы для синхронизации
	tokenMutex              sync.Mutex
	mediaGroupCacheMu       sync.Mutex
	mediaGroupCleanupTicker *time.Ticker

	// OAuth-конфигурация, которую заполняем в main()
	oauthConfig *oauth2.Config

	// Состояние OAuth
	oauthState = "state-token"
	authCodeCh = make(chan string)

	// Кэш для медиагрупп (используется для объединения фото из группы)
	mediaGroupCache = make(map[string]*MediaGroupData)

	// Общие каналы для обработки заданий, если потребуется расширение
	taskQueue   = make(chan FileTask, 100)
	resultsChan = make(chan FileResult, 100)

	// Для ограничения количества одновременных горутин (если нужно)
	semaphore = make(chan struct{}, 100)
	wg        sync.WaitGroup
)

//
// ========================
// Структуры данных 📦
// ========================

// ParsedData хранит данные, которые будут записаны в Google Sheets
type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string
}

// TokenInfo для сохранения токена OAuth в виде JSON
type TokenInfo struct {
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type"`
	RefreshToken string    `json:"refresh_token"`
	Expiry       time.Time `json:"expiry"`
}

// MediaGroupData – данные, связанные с группой медиа (фото)
type MediaGroupData struct {
	Files            map[string]*tgbotapi.PhotoSize // Кэш фото (ключ – FileID)
	Caption          string                         // Подпись к первому фото
	Address          string
	Amount           string
	Comment          string
	FirstMessageTime time.Time // Время получения первого сообщения группы
	LastUpdated      time.Time // Время последнего обновления группы
	UserID           int64
	ChatID           int64
	Username         string
	IsProcessing     bool // Флаг, что группа уже обрабатывается
}

// FileTask и FileResult можно использовать для расширенной обработки файлов (если понадобится)
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

// Для парсинга подписи по регулярке
type fieldMatch struct {
	field string
	start int
	end   int
}

//
// ========================
// Функции для загрузки настроек из переменных окружения 🔧
// ========================

func loadEnvVars() (string, string, string, int64, string, string, string) {
	telegramToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN не установлен ❌")
	}
	spreadsheetId := os.Getenv("GOOGLE_SHEET_ID")
	if spreadsheetId == "" {
		log.Fatal("GOOGLE_SHEET_ID не установлен ❌")
	}
	driveFolderId := os.Getenv("GOOGLE_DRIVE_FOLDER_ID")
	if driveFolderId == "" {
		log.Fatal("GOOGLE_DRIVE_FOLDER_ID не установлен ❌")
	}
	adminIDStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	if adminIDStr == "" {
		log.Fatal("ADMIN_CHAT_ID не установлен ❌")
	}
	adminID, err := strconv.ParseInt(adminIDStr, 10, 64)
	if err != nil {
		log.Fatalf("Неверный формат ADMIN_CHAT_ID: %v", err)
	}
	googleClientID := os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret := os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	if googleClientID == "" || googleClientSecret == "" {
		log.Fatal("GOOGLE_OAUTH_CLIENT_ID и GOOGLE_OAUTH_CLIENT_SECRET не установлены ❌")
	}
	webhookURL := os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		log.Fatal("WEBHOOK_URL не установлен ❌")
	}
	return telegramToken, spreadsheetId, driveFolderId, adminID, googleClientID, googleClientSecret, webhookURL
}

//
// ========================
// Функции OAuth и работы с токеном 🔑
// ========================

// getOAuthClient возвращает HTTP клиент с актуальным OAuth токеном
func getOAuthClient(config *oauth2.Config) (*http.Client, error) {
	tokenMutex.Lock()
	token, err := loadTokenFromEnv()
	tokenMutex.Unlock()
	if err == nil {
		// Если токен скоро истечёт, пробуем обновить его
		if time.Until(token.Expiry) < tokenRefreshWindow && token.RefreshToken != "" {
			newToken, err := refreshToken(config, token)
			if err == nil {
				_ = saveTokenToEnv(newToken)
				return config.Client(context.Background(), newToken), nil
			}
		} else if token.Valid() {
			return config.Client(context.Background(), token), nil
		}
	}

	// Запуск локального сервера для OAuth авторизации
	serverErrCh := make(chan error, 1)
	server := startOAuthServer(serverErrCh)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	authURL := config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	fmt.Printf("👉 Перейдите по ссылке для авторизации:\n%v\n", authURL)

	select {
	case code := <-authCodeCh:
		token, err := config.Exchange(context.Background(), code)
		if err != nil {
			return nil, fmt.Errorf("ошибка обмена кода: %v", err)
		}
		_ = saveTokenToEnv(token)
		return config.Client(context.Background(), token), nil
	case err := <-serverErrCh:
		return nil, fmt.Errorf("ошибка OAuth сервера: %v", err)
	case <-time.After(5 * time.Minute):
		return nil, errors.New("превышено время ожидания авторизации ⏰")
	}
}

// startOAuthServer запускает локальный HTTP сервер для получения кода авторизации
func startOAuthServer(errCh chan<- error) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != oauthState {
			http.Error(w, "Неверный state", http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "Код не найден", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "Авторизация прошла успешно. Закройте окно 😊")
		authCodeCh <- code
	})
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	return server
}

// saveTokenToEnv сохраняет токен в переменную окружения (в base64)
func saveTokenToEnv(token *oauth2.Token) error {
	if token == nil {
		return errors.New("пустой токен")
	}
	tokenInfo := TokenInfo{
		AccessToken:  token.AccessToken,
		TokenType:    token.TokenType,
		RefreshToken: token.RefreshToken,
		Expiry:       token.Expiry,
	}
	// Если новый токен не содержит RefreshToken – сохраняем старый, если он есть
	if token.RefreshToken == "" && os.Getenv("GOOGLE_OAUTH_TOKEN") != "" {
		oldToken, err := loadTokenFromEnv()
		if err == nil && oldToken.RefreshToken != "" {
			tokenInfo.RefreshToken = oldToken.RefreshToken
		}
	}
	tokenJSON, err := json.Marshal(tokenInfo)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга: %v", err)
	}
	encodedToken := base64.StdEncoding.EncodeToString(tokenJSON)
	return os.Setenv("GOOGLE_OAUTH_TOKEN", encodedToken)
}

// loadTokenFromEnv загружает токен из переменной окружения
func loadTokenFromEnv() (*oauth2.Token, error) {
	tokenStr := os.Getenv("GOOGLE_OAUTH_TOKEN")
	if tokenStr == "" {
		return nil, errors.New("токен не найден")
	}
	tokenJSON, err := base64.StdEncoding.DecodeString(tokenStr)
	if err != nil {
		return nil, fmt.Errorf("ошибка декодирования: %v", err)
	}
	var tokenInfo TokenInfo
	if err := json.Unmarshal(tokenJSON, &tokenInfo); err != nil {
		return nil, fmt.Errorf("ошибка анмаршалинга: %v", err)
	}
	return &oauth2.Token{
		AccessToken:  tokenInfo.AccessToken,
		TokenType:    tokenInfo.TokenType,
		RefreshToken: tokenInfo.RefreshToken,
		Expiry:       tokenInfo.Expiry,
	}, nil
}

// refreshToken пытается обновить OAuth токен с несколькими повторами
func refreshToken(config *oauth2.Config, token *oauth2.Token) (*oauth2.Token, error) {
	var newToken *oauth2.Token
	var err error
	for i := 0; i < maxRetries; i++ {
		newToken, err = config.TokenSource(context.Background(), token).Token()
		if err == nil {
			if newToken.RefreshToken == "" {
				newToken.RefreshToken = token.RefreshToken
			}
			return newToken, nil
		}
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return nil, fmt.Errorf("не удалось обновить токен после %d попыток: %v", maxRetries, err)
}

//
// ========================
// Функции для работы с Google Drive и Sheets 📁📊
// ========================

// ensureObjectFolder ищет или создаёт папку на Google Drive для заданного объекта (например, адрес)
func ensureObjectFolder(service *drive.Service, parentFolderId, objectName string) (string, error) {
	sanitized := strings.TrimSpace(objectName)
	if sanitized == "" {
		sanitized = "Разное"
	}
	sanitized = sanitizeFileName(sanitized)
	query := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and trashed = false", sanitized, parentFolderId)

	var fileList *drive.FileList
	var err error
	for i := 0; i < maxRetries; i++ {
		fileList, err = service.Files.List().Q(query).Fields("files(id, name)").PageSize(10).Do()
		if err == nil {
			break
		}
		service, _ = refreshDriveService(service, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	if err != nil {
		return "", fmt.Errorf("поиск папки не удался: %v", err)
	}
	if len(fileList.Files) > 0 {
		return fileList.Files[0].Id, nil
	}

	// Если папка не найдена, создаём новую
	folder := &drive.File{
		Name:     sanitized,
		Parents:  []string{parentFolderId},
		MimeType: "application/vnd.google-apps.folder",
	}
	var created *drive.File
	for i := 0; i < maxRetries; i++ {
		created, err = service.Files.Create(folder).Fields("id, name").Do()
		if err == nil {
			break
		}
		service, _ = refreshDriveService(service, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	if err != nil {
		return "", fmt.Errorf("создание папки не удалось: %v", err)
	}
	return created.Id, nil
}

// uploadFileToDrive загружает локальный файл в Google Drive
func uploadFileToDrive(service *drive.Service, filePath, fileName, folderId string) (string, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		f, err := os.Open(filePath)
		if err != nil {
			return "", fmt.Errorf("ошибка открытия файла: %v", err)
		}
		defer f.Close()
		driveFile := &drive.File{
			Name:    fileName,
			Parents: []string{folderId},
		}
		res, err := service.Files.Create(driveFile).Media(f).Fields("webViewLink").Do()
		if err == nil {
			return res.WebViewLink, nil
		}
		lastErr = err
		service, _ = refreshDriveService(service, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return "", fmt.Errorf("загрузка файла не удалась: %v", lastErr)
}

// refreshDriveService обновляет сервис Google Drive, если токен истёк
func refreshDriveService(service *drive.Service, originalErr error) (*drive.Service, error) {
	if strings.Contains(originalErr.Error(), "oauth2: token expired") {
		newClient, err := getOAuthClient(oauthConfig)
		if err != nil {
			return service, fmt.Errorf("обновление клиента не удалось: %v", err)
		}
		newService, err := drive.NewService(context.Background(), option.WithHTTPClient(newClient))
		if err != nil {
			return service, fmt.Errorf("создание нового сервиса не удалось: %v", err)
		}
		return newService, nil
	}
	return service, originalErr
}

// appendToSheet записывает данные в Google Sheets
func appendToSheet(service *sheets.Service, spreadsheetId string, data ParsedData) error {
	// Обратите внимание: для интерактивных ссылок мы используем пробел вместо запятой
	values := []interface{}{data.Date, data.Username, data.Address, data.Amount, data.Comment, data.DriveLink}
	vr := &sheets.ValueRange{Values: [][]interface{}{values}}

	// Получаем текущие данные для определения последней строки
	resp, err := service.Spreadsheets.Values.Get(spreadsheetId, sheetIDRange).Do()
	if err != nil {
		return fmt.Errorf("получение данных не удалось: %v", err)
	}
	lastRow := 1
	if len(resp.Values) > 0 {
		lastRow = len(resp.Values) + 1
	}
	rangeStr := fmt.Sprintf(sheetUpdateRange, lastRow, lastRow)
	_, err = service.Spreadsheets.Values.Update(spreadsheetId, rangeStr, vr).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		return fmt.Errorf("обновление Sheets не удалось: %v", err)
	}

	// Можно добавить дополнительное форматирование ячеек, если требуется
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
	return err
}

// keepAlive отправляет периодические GET-запросы на webhookURL, чтобы не "засыпал" сервер
func keepAlive(webhookURL string) {
	ticker := time.NewTicker(10 * time.Minute)
	go func() {
		for range ticker.C {
			resp, err := http.Get(webhookURL)
			if err != nil {
				continue
			}
			resp.Body.Close()
		}
	}()
}

//
// ========================
// Функции обработки сообщений Telegram 📲
// ========================

// handleSinglePhotoMessage обрабатывает сообщение с одним фото
func handleSinglePhotoMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string) {
	// Проверка наличия подписи
	if message.Caption == "" {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Укажи адрес и сумму в подписи к фото в формате:\nАдрес: ...\nСумма: ..."))
		return
	}

	// Парсим подпись
	addr, amt, comm, err := parseMessage(message.Caption)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Не удалось распознать подпись. Укажи адрес и сумму в формате:\nАдрес: ...\nСумма: ..."))
		return
	}

	// Проверяем обязательные поля
	if addr == "" || amt == "" {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Обязательно укажи адрес и сумму в подписи!"))
		return
	}

	// Получаем фото наилучшего качества (последний в срезе)
	bestPhoto := message.Photo[len(message.Photo)-1]

	// Получаем прямую ссылку на файл
	fileURL, err := bot.GetFileDirectURL(bestPhoto.FileID)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Не удалось получить фото: "+err.Error()))
		return
	}

	// Загружаем фото в Google Drive
	fileID, err := uploadPhotoToDrive(driveService, fileURL, parentFolderId, fmt.Sprintf("check_%s", time.Now().Format("20060102_150405")))
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Не удалось загрузить фото в Google Drive: "+err.Error()))
		return
	}

	// Формируем ссылку на файл
	fileLink := fmt.Sprintf("https://drive.google.com/file/d/%s/view", fileID)

	// Готовим данные для записи в таблицу
	timestamp := time.Now().Format("02.01.2006 15:04:05")
	username := getFullName(message.From)
	row := []interface{}{timestamp, username, addr, amt, comm, fileLink}

	// Записываем строку в Google Sheets
	appendRange := "Чеки!A:F"
	valueRange := &sheets.ValueRange{Values: [][]interface{}{row}}
	_, err = sheetsService.Spreadsheets.Values.Append(spreadsheetId, appendRange, valueRange).
		ValueInputOption("USER_ENTERED").
		InsertDataOption("INSERT_ROWS").
		Do()
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Не удалось записать данные в таблицу: "+err.Error()))
		return
	}

	// Сообщаем об успехе
	bot.Send(tgbotapi.NewMessage(message.Chat.ID, fmt.Sprintf("✅ Чек успешно загружен!\nАдрес: %s\nСумма: %s", addr, amt)))
}

// handleMediaGroupMessage обрабатывает сообщения с группой фото (медиагруппа)
func handleMediaGroupMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	// Если фото отсутствуют – сообщаем и выходим
	if len(message.Photo) == 0 {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Сообщение не содержит фотографий."))
		return
	}

	// Если сообщение не относится к медиагруппе, обрабатываем как единичное фото
	if message.MediaGroupID == "" {
		handleSinglePhotoMessage(bot, message, sheetsService, spreadsheetId, driveService, parentFolderId)
		return
	}

	// Работа с кэшем медиагруппы для объединения фото из одной группы
	mediaGroupCacheMu.Lock()
	groupData, exists := mediaGroupCache[message.MediaGroupID]
	if !exists {
		// Первое сообщение в группе – пытаемся распарсить подпись, если она есть
		caption := message.Caption
		var addr, amt, comm string
		if caption != "" {
			var err error
			addr, amt, comm, err = parseMessage(caption)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Не удалось распознать подпись. Укажи адрес и сумму в формате:\nАдрес: ...\nСумма: ..."))
				mediaGroupCacheMu.Unlock()
				return
			}
			if addr == "" || amt == "" {
				bot.Send(tgbotapi.NewMessage(message.Chat.ID, "❗️ Обязательно укажи адрес и сумму в подписи к первому фото группы!"))
				mediaGroupCacheMu.Unlock()
				return
			}
		}
		// Создаём новую запись для медиагруппы
		groupData = &MediaGroupData{
			Files:            make(map[string]*tgbotapi.PhotoSize),
			Caption:          caption,
			Address:          addr,
			Amount:           amt,
			Comment:          comm,
			FirstMessageTime: time.Now(),
			LastUpdated:      time.Now(),
			UserID:           message.From.ID,
			ChatID:           message.Chat.ID,
			Username:         getFullName(message.From),
			IsProcessing:     false,
		}
		mediaGroupCache[message.MediaGroupID] = groupData
	}
	// Добавляем текущее фото в кэш (если ещё не добавлено)
	bestPhoto := message.Photo[len(message.Photo)-1]
	if _, ok := groupData.Files[bestPhoto.FileID]; !ok {
		groupData.Files[bestPhoto.FileID] = &bestPhoto
		groupData.LastUpdated = time.Now()
	}
	mediaGroupID := message.MediaGroupID

	// Проверяем условия для старта обработки группы:
	// 1. Прошло не менее 1 сек с первого фото (даём время на приход остальных фото)
	// 2. Группа ещё не в обработке
	// 3. Либо собрано 10 фото, либо прошло более 2 сек
	timeSinceFirst := time.Since(groupData.FirstMessageTime)
	shouldProcess := false
	if !groupData.IsProcessing && timeSinceFirst >= 1*time.Second &&
		(len(groupData.Files) >= 10 || timeSinceFirst >= 2*time.Second) {
		shouldProcess = true
		groupData.IsProcessing = true
	}
	mediaGroupCacheMu.Unlock()

	// Если пора обрабатывать группу – запускаем отдельную горутину
	if shouldProcess {
		go processMediaGroupOptimized(bot, mediaGroupID, sheetsService, spreadsheetId, driveService, parentFolderId, adminID)
	}
}

// processMediaGroupOptimized – обработка медиагруппы с параллельной загрузкой фото
func processMediaGroupOptimized(bot *tgbotapi.BotAPI, mediaGroupID string, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	// Немного ждём, чтобы убедиться, что все фото получены
	time.Sleep(500 * time.Millisecond)

	mediaGroupCacheMu.Lock()
	groupData, exists := mediaGroupCache[mediaGroupID]
	if !exists || len(groupData.Files) == 0 {
		mediaGroupCacheMu.Unlock()
		return
	}
	// Создаём локальные копии необходимых данных
	addr := groupData.Address
	amt := groupData.Amount
	comm := groupData.Comment
	chatID := groupData.ChatID
	username := groupData.Username

	// Собираем фото в срез для сортировки по качеству (по убыванию площади)
	var photos []*tgbotapi.PhotoSize
	for _, photo := range groupData.Files {
		photos = append(photos, photo)
	}
	sort.Slice(photos, func(i, j int) bool {
		return photos[i].Width*photos[i].Height > photos[j].Width*photos[j].Height
	})
	mediaGroupCacheMu.Unlock()

	// Если обязательные данные отсутствуют, уведомляем пользователя
	if addr == "" || amt == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Укажи адрес и сумму в подписи к первому фото группы!"))
		return
	}

	// Находим или создаём папку для объекта (адрес) на Google Drive
	folderID, err := ensureObjectFolder(driveService, parentFolderId, addr)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка обработки объекта: "+err.Error()))
		return
	}

	// Формируем базовое имя файла и формат даты
	sanitized := sanitizeFileName(addr)
	moscow := time.FixedZone("MSK", 3*3600)
	dateFormatted := time.Now().In(moscow).Format("02.01.2006")

	// Канал для сбора ссылок загруженных файлов
	uploadResults := make(chan string, len(photos))
	var uploadWg sync.WaitGroup
	uploadSemaphore := make(chan struct{}, maxConcurrentUploads)

	// Параллельная загрузка каждого фото
	for i, photo := range photos {
		uploadWg.Add(1)
		go func(index int, p *tgbotapi.PhotoSize) {
			defer uploadWg.Done()
			uploadSemaphore <- struct{}{} // Захватываем слот
			defer func() { <-uploadSemaphore }()

			// Получаем информацию о файле
			fileInfo, err := bot.GetFile(tgbotapi.FileConfig{FileID: p.FileID})
			if err != nil {
				return
			}
			fileURL := fileInfo.Link(bot.Token)
			// Формируем имя файла
			fileName := sanitizeFileName(fmt.Sprintf("%s_%s_%02d_%s.jpg", sanitized, dateFormatted, index+1, amt))
			// Загружаем файл (общая функция загрузки)
			link, err := downloadAndUploadFile(fileURL, fileName, driveService, folderID)
			if err != nil {
				return
			}
			uploadResults <- link
		}(i, photo)
	}
	go func() {
		uploadWg.Wait()
		close(uploadResults)
	}()

	// Собираем все ссылки (для интерактивности ссылки разделяем пробелом)
	var links []string
	for link := range uploadResults {
		links = append(links, link)
	}
	if len(links) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Не удалось загрузить фотографии."))
		return
	}

	// Готовим данные для записи в таблицу
	parsedData := ParsedData{
		Address:  addr,
		Amount:   amt,
		Comment:  comm,
		Username: username,
		Date:     time.Now().In(moscow).Format("02/01/2006 15:04:05"),
		// Используем пробел для разделения ссылок, чтобы они стали интерактивными
		DriveLink: strings.Join(links, " "),
	}
	if err := appendToSheet(sheetsService, spreadsheetId, parsedData); err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка записи данных в таблицу: "+err.Error()))
		return
	}

	// Удаляем обработанную медиагруппу из кэша
	mediaGroupCacheMu.Lock()
	delete(mediaGroupCache, mediaGroupID)
	mediaGroupCacheMu.Unlock()

	// Уведомляем пользователя об успешной обработке группы
	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Успешно обработано %d из %d фото.", len(links), len(photos))))
}

// downloadAndUploadFile скачивает файл по URL во временный файл и загружает его на Google Drive
func downloadAndUploadFile(fileURL, fileName string, driveService *drive.Service, folderID string) (string, error) {
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("❗️ Ошибка скачивания: %v", err)
	}
	defer resp.Body.Close()

	tmpFile, err := os.CreateTemp("", "tg_photo_*")
	if err != nil {
		return "", fmt.Errorf("❗️ Ошибка создания temp файла: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err = io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("❗️ Ошибка копирования: %v", err)
	}
	tmpFile.Close()

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		file, err := os.Open(tmpFile.Name())
		if err != nil {
			return "", fmt.Errorf("❗️ Ошибка открытия файла: %v", err)
		}
		driveFile := &drive.File{
			Name:    fileName,
			Parents: []string{folderID},
		}
		res, err := driveService.Files.Create(driveFile).Media(file).Fields("webViewLink").Do()
		file.Close()
		if err == nil {
			return res.WebViewLink, nil
		}
		lastErr = err
		driveService, _ = refreshDriveService(driveService, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return "", fmt.Errorf("❗️ Загрузка файла не удалась после %d попыток: %v", maxRetries, lastErr)
}

// notifyAdminAboutSheetError уведомляет администратора о критической ошибке записи в таблицу
func notifyAdminAboutSheetError(bot *tgbotapi.BotAPI, adminID int64, err error, mediaGroupID string) {
	msg := tgbotapi.NewMessage(adminID, fmt.Sprintf("⚠️ Ошибка Sheets для медиагруппы %s: %v", mediaGroupID, err))
	_, _ = bot.Send(msg)
}

// getFullName возвращает полное имя пользователя (FirstName + LastName)
func getFullName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

//
// ========================
// Функции для парсинга сообщений и очистки строк 🔍
// ========================

// removeLeadingKeyword удаляет ключевое слово из начала строки
func removeLeadingKeyword(text string, keywords []string) string {
	trimmed := strings.TrimSpace(text)
	lower := strings.ToLower(trimmed)
	for _, kw := range keywords {
		if strings.HasPrefix(lower, kw) {
			return strings.TrimSpace(trimmed[len(kw):])
		}
	}
	return trimmed
}

// fallbackParse выполняет альтернативный разбор сообщения, если основной парсинг не сработал
func fallbackParse(message string) (string, string, string, error) {
	if strings.Contains(message, "\n") {
		lines := strings.Split(message, "\n")
		var addr, amt, comm string
		if len(lines) > 0 {
			addr = removeLeadingKeyword(strings.TrimSpace(lines[0]), fieldKeywords["address"])
		}
		if len(lines) > 1 {
			amt = removeLeadingKeyword(strings.TrimSpace(lines[1]), fieldKeywords["amount"])
			amt = cleanAmount(amt)
		}
		if len(lines) > 2 {
			comm = removeLeadingKeyword(strings.TrimSpace(strings.Join(lines[2:], "\n")), fieldKeywords["comment"])
		}
		if addr == "" || amt == "" {
			return "", "", "", errors.New("обязательные поля не найдены")
		}
		return addr, amt, comm, nil
	}
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
		addr := removeLeadingKeyword(addressPart, fieldKeywords["address"])
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
			comm := removeLeadingKeyword(strings.TrimSpace(amountPart[commentIdx:]), fieldKeywords["comment"])
			amt = cleanAmount(amt)
			return addr, amt, comm, nil
		}
		return addr, cleanAmount(amountPart), "", nil
	}
	addr := removeLeadingKeyword(message, fieldKeywords["address"])
	return addr, "", "", errors.New("сумма не найдена")
}

// parseMessage анализирует сообщение и возвращает адрес, сумму и комментарий
func parseMessage(message string) (string, string, string, error) {
	if strings.TrimSpace(message) == "" {
		return "", "", "", errors.New("пустое сообщение")
	}
	if strings.Contains(message, ":") || strings.Contains(message, "=") {
		normalized := strings.Join(strings.Fields(message), " ")
		var matches []fieldMatch
		for field, keywords := range fieldKeywords {
			for _, kw := range keywords {
				pattern := fmt.Sprintf("(?i)%s\\s*[:=]\\s*", regexp.QuoteMeta(kw))
				re := regexp.MustCompile(pattern)
				locs := re.FindAllStringIndex(normalized, -1)
				for _, loc := range locs {
					matches = append(matches, fieldMatch{field: field, start: loc[0], end: loc[1]})
				}
			}
		}
		if len(matches) > 0 {
			sort.Slice(matches, func(i, j int) bool { return matches[i].start < matches[j].start })
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
			addr := fieldValues["address"]
			amt := cleanAmount(fieldValues["amount"])
			comm := fieldValues["comment"]
			if addr == "" || amt == "" {
				return fallbackParse(message)
			}
			return addr, amt, comm, nil
		}
		return fallbackParse(message)
	}
	return fallbackParse(message)
}

// cleanAmount очищает строку с суммой от лишних символов и заменяет точку на запятую
func cleanAmount(amount string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	cleaned := re.ReplaceAllString(amount, "")
	return strings.ReplaceAll(cleaned, ".", ",")
}

// sanitizeFileName удаляет недопустимые символы из имени файла
func sanitizeFileName(name string) string {
	re := regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s\.-]`)
	sanitized := re.ReplaceAllString(name, "_")
	sanitized = regexp.MustCompile(`\s+`).ReplaceAllString(sanitized, " ")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	sanitized = regexp.MustCompile(`_+`).ReplaceAllString(sanitized, "_")
	return strings.Trim(sanitized, "_")
}

//
// ========================
// HTTP обработчик для Webhook и старт бота 🚀
// ========================

// setupHandler настраивает HTTP обработчик для приема обновлений от Telegram
func setupHandler(bot *tgbotapi.BotAPI, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	// Инициализируем очистку устаревших медиагрупп
	initMediaGroupHandler()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			bytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}
			var update tgbotapi.Update
			err = json.Unmarshal(bytes, &update)
			if err != nil {
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}
			if update.Message != nil {
				// Обработка команд (приветствие и помощь)
				if update.Message.IsCommand() {
					switch update.Message.Command() {
					case "start", "help":
						helpText := `👋 Привет! Я бот для отслеживания чеков!
						
Отправь мне фотографию чека с подписью, где:
• **Адрес**: адрес объекта 🏠
• **Сумма**: стоимость или оплата 💰
• **Комментарий**: опционально, можно добавить заметку 😉
						
Ты можешь отправить как одиночное фото, так и группу фото (до 10 штук)! Подпись указывай только к первому фото группы.
						
Давай попробуем?`
						msg := tgbotapi.NewMessage(update.Message.Chat.ID, helpText)
						msg.ParseMode = "Markdown"
						bot.Send(msg)
					}
				} else if update.Message.Photo != nil {
					// Если получено фото – обрабатываем в отдельной горутине
					go handleMediaGroupMessage(bot, update.Message, sheetsService, spreadsheetId, driveService, parentFolderId, adminID)
				}
			}
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
		}
	})
}

// initMediaGroupHandler запускает периодическую очистку кэша медиагрупп
func initMediaGroupHandler() {
	mediaGroupCleanupTicker = time.NewTicker(1 * time.Minute)
	go func() {
		for range mediaGroupCleanupTicker.C {
			cleanupExpiredMediaGroups()
		}
	}()
}

// cleanupExpiredMediaGroups удаляет устаревшие данные медиагрупп
func cleanupExpiredMediaGroups() {
	mediaGroupCacheMu.Lock()
	defer mediaGroupCacheMu.Unlock()
	now := time.Now()
	for id, data := range mediaGroupCache {
		if now.Sub(data.LastUpdated) > 2*time.Minute {
			delete(mediaGroupCache, id)
		}
	}
}

// ========================
// Функция загрузки фото напрямую в Google Drive (для одиночных фото) 📸
// ========================
func uploadPhotoToDrive(driveService *drive.Service, fileURL, parentFolderId, filename string) (string, error) {
	// Скачиваем файл по URL
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	driveFile := &drive.File{
		Name:     filename,
		MimeType: "image/jpeg",
	}
	if parentFolderId != "" {
		driveFile.Parents = []string{parentFolderId}
	}
	file, err := driveService.Files.Create(driveFile).Media(resp.Body).Do()
	if err != nil {
		return "", err
	}
	return file.Id, nil
}

// ========================
// Главная функция main() – точка входа в программу 🚀
// ========================
func main() {
	// Загружаем переменные окружения
	telegramToken, spreadsheetId, driveFolderId, adminID, googleClientID, googleClientSecret, webhookURL := loadEnvVars()

	// Настраиваем OAuth для Google API
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

	// Периодически обновляем OAuth клиент в отдельной горутине
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			_, err := getOAuthClient(oauthConfig)
			if err != nil {
				// Можно логировать ошибку, если потребуется
			}
		}
	}()

	// Получаем HTTP клиент с актуальным OAuth токеном
	client, err := getOAuthClient(oauthConfig)
	if err != nil {
		log.Fatalf("OAuth клиент не получен: %v", err)
	}

	// Инициализируем сервисы Google Sheets и Drive
	sheetsService, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Sheets сервис не создан: %v", err)
	}
	driveService, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Drive сервис не создан: %v", err)
	}

	// Создаём бота Telegram
	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Panic(err)
	}
	bot.Debug = true

	// Настраиваем webhook для Telegram
	parsedWebhookURL, err := url.Parse(webhookURL)
	if err != nil {
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}
	webhookConfig := tgbotapi.WebhookConfig{URL: parsedWebhookURL, MaxConnections: 40}
	_, err = bot.Request(webhookConfig)
	if err != nil {
		log.Fatalf("Webhook не установлен: %v", err)
	}

	// Запускаем функцию keepAlive, чтобы сервер не "засыпал"
	keepAlive(webhookURL)

	// Настраиваем HTTP обработчик для получения обновлений от Telegram
	setupHandler(bot, sheetsService, spreadsheetId, driveService, driveFolderId, adminID)

	// Запускаем HTTP сервер
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{Addr: ":" + port}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP сервер не запущен: %v", err)
		}
	}()

	// Ждём сигнала завершения (Ctrl+C или SIGTERM)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	wg.Wait()
}
