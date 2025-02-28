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

type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string
}

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
)

const maxGoroutines = 10

var semaphore = make(chan struct{}, maxGoroutines)

type TokenInfo struct {
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type"`
	RefreshToken string    `json:"refresh_token"`
	Expiry       time.Time `json:"expiry"`
}

const (
	maxRetries         = 3
	retryDelay         = 2
	tokenRefreshWindow = 5 * time.Minute
)

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

func getClient(config *oauth2.Config) (*http.Client, error) {
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

func getFullName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

func parseMessage(message string) (address string, amount string, comment string, err error) {
	if message == "" {
		return "", "", "", errors.New("пустое сообщение")
	}

	lines := strings.Split(message, "\n")
	var addr, amt, comm []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		found := false

		for field, keywords := range fieldKeywords {
			for _, keyword := range keywords {
				patterns := []string{
					fmt.Sprintf(`(?i)(%s\s*[:=-])\s*(.+)`, regexp.QuoteMeta(keyword)),
					fmt.Sprintf(`(?i)^%s\s+(.+)`, regexp.QuoteMeta(keyword)),
				}

				for _, pattern := range patterns {
					re := regexp.MustCompile(pattern)
					matches := re.FindStringSubmatch(line)
					if len(matches) > 0 {
						value := strings.TrimSpace(matches[len(matches)-1])
						if value != "" {
							switch field {
							case "address":
								addr = append(addr, value)
							case "amount":
								value = cleanAmount(value)
								amt = append(amt, value)
							case "comment":
								comm = append(comm, value)
							}
							found = true
							break
						}
					}
				}
				if found {
					break
				}
			}
			if found {
				break
			}
		}
	}

	if len(addr) == 0 || len(amt) == 0 {
		return "", "", "", errors.New("не удалось найти обязательные поля: адрес и сумма")
	}

	return strings.Join(addr, " "), strings.Join(amt, " "), strings.Join(comm, " "), nil
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

func sendMessageToAdmin(bot *tgbotapi.BotAPI, adminID int64, message string) {
	msg := tgbotapi.NewMessage(adminID, message)
	_, err := bot.Send(msg)
	if err != nil {
		if strings.Contains(err.Error(), "bot can't initiate conversation with a user") {
			log.Printf("Администратор должен начать диалог с ботом первым: /start")
			return
		}
		log.Printf("Ошибка при отправке сообщения админу: %v", err)
	}
}

func handlePhotoMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, driveFolderId string, adminID int64) {
	if message.Photo == nil && message.Video == nil && message.Document == nil {
		log.Println("Сообщение не содержит медиа")
		reply := tgbotapi.NewMessage(message.Chat.ID, "Пожалуйста, прикрепите фотографию чека. Используйте /help для просмотра формата.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Пользователь %s отправил сообщение без медиа файла", getFullName(message.From)))
		return
	}

	comment := message.Caption
	if comment == "" {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Пожалуйста, добавьте описание к фотографии. Используйте /help для просмотра формата.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Пользователь %s отправил фото без описания", getFullName(message.From)))
		return
	}

	address, amount, commentText, parseErr := parseMessage(comment)
	if parseErr != nil {
		log.Printf("Ошибка при парсинге сообщения: %v", parseErr)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так, проверьте правильность заполнения. Обратите внимание на шаблон сообщения и наличие фото. Попробуйте /start чтобы посмотреть справку.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при парсинге сообщения: %v", parseErr))
		return
	}

	moscowOffset := int((3 * time.Hour).Seconds())
	moscowTime := time.Unix(int64(message.Date), 0).UTC().Add(time.Duration(moscowOffset) * time.Second)
	dateFormatted := moscowTime.Format("02.01.2006") // Изменен формат даты на точки вместо слешей

	username := getFullName(message.From)

	photo := message.Photo[len(message.Photo)-1]
	fileID := photo.FileID
	file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
	if err != nil {
		log.Printf("Ошибка при получении файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при загрузке фотографии. Попробуйте /start чтобы посмотреть справку.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при получении файла: %v", err))
		return
	}

	fileURL := file.Link(bot.Token)
	resp, err := http.Get(fileURL)
	if err != nil {
		log.Printf("Ошибка при скачивании файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при скачивании фотографии. Попробуйте /start чтобы посмотреть справку.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при скачивании файла: %v", err))
		return
	}
	defer resp.Body.Close()

	sanitizedAddress := sanitizeFileName(address)
	fileName := fmt.Sprintf("%s_%s.jpg", sanitizedAddress, dateFormatted)

	tmpFile, err := os.CreateTemp("", "receipt_*_"+fileName)
	if err != nil {
		log.Printf("Не удалось создать временный файл: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при сохранении фотографии. Попробуйте /start чтобы посмотреть справку.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Не удалось создать временный файл: %v", err))
		return
	}
	defer os.Remove(tmpFile.Name())

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		log.Printf("Ошибка при сохранении файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при сохранении фотографии. Попробуйте /start чтобы посмотреть справку.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при сохранении файла: %v", err))
		return
	}

	driveLink, err := uploadFileToDrive(driveService, tmpFile.Name(), fileName, driveFolderId)
	if err != nil {
		log.Printf("Ошибка при загрузке файла на Drive: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при загрузке фотографии на Drive. Попробуйте /start чтобы посмотреть справку.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при загрузке файла на Google Drive: %v", err))
		return
	}

	parsedData := ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  username,
		Date:      moscowTime.Format("02/01/2006 15:04:05"), // Сохраняем полный формат для таблицы
		DriveLink: driveLink,
	}

	err = appendToSheet(sheetsService, spreadsheetId, parsedData)
	if err != nil {
		log.Printf("Ошибка при записи в Google Sheets: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при записи данных в таблицу. Попробуйте /start чтобы посмотреть справку.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при записи в Google Sheets: %v", err))
		return
	}

	reply := tgbotapi.NewMessage(message.Chat.ID, "Чек успешно добавлен в таблицу.")
	bot.Send(reply)
	log.Println("Данные успешно добавлены в Google Sheets и файл загружен на Drive")
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

		if strings.Contains(err.Error(), "oauth2: token expired") {
			newClient, err := getClient(oauthConfig)
			if err != nil {
				log.Printf("Не удалось обновить клиент: %v", err)
				continue
			}

			service, err = drive.NewService(context.Background(), option.WithHTTPClient(newClient))
			if err != nil {
				log.Printf("Не удалось создать новый Drive сервис: %v", err)
				continue
			}
		}

		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}

	return "", fmt.Errorf("не удалось загрузить файл после %d попыток: %v", maxRetries, lastErr)
}

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

	resp, err := service.Spreadsheets.Values.Get(spreadsheetId, "'Чеки'!B:B").Do()
	if err != nil {
		return fmt.Errorf("не удалось получить данные из Google Sheets: %v", err)
	}

	lastRow := 1
	if len(resp.Values) > 0 {
		lastRow = len(resp.Values) + 1
	}

	rangeStr := fmt.Sprintf("'Чеки'!B%d:G%d", lastRow, lastRow)

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
						SheetId:          1051413829,
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

func handleMediaGroupMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	if len(message.Photo) == 0 && len(message.MediaGroupID) == 0 {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Сообщение не содержит фотографии. Попробуйте снова.")
		bot.Send(reply)
		return
	}

	comment := message.Caption
	address, amount, commentText, parseErr := parseMessage(comment)
	if parseErr != nil {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Не удалось распознать подпись к фото. Проверьте формат данных.")
		bot.Send(reply)
		return
	}

	// Проверим или создадим папку для объекта
	objectFolderID, err := ensureObjectFolder(driveService, parentFolderId, address)
	if err != nil {
		log.Printf("Ошибка создания папки для объекта: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Ошибка обработки объекта. Попробуйте позже.")
		bot.Send(reply)
		return
	}

	// Сохраним ссылки на фото
	var links []string
	for _, photo := range message.Photo {
		photoFile, err := bot.GetFile(tgbotapi.FileConfig{FileID: photo.FileID})
		if err != nil {
			log.Printf("Ошибка загрузки файла: %v", err)
			continue
		}
		fileURL := photoFile.Link(bot.Token)
		resp, err := http.Get(fileURL)
		if err != nil {
			log.Printf("Ошибка скачивания файла: %v", err)
			continue
		}
		defer resp.Body.Close()

		fileName := sanitizeFileName(fmt.Sprintf("%s_%s.jpg", address, time.Now().Format("20060102_150405")))
		tmpFile, err := os.CreateTemp("", fileName)
		if err != nil {
			log.Printf("Ошибка создания временного файла: %v", err)
			continue
		}
		defer os.Remove(tmpFile.Name())

		_, err = io.Copy(tmpFile, resp.Body)
		if err != nil {
			log.Printf("Ошибка копирования файла: %v", err)
			continue
		}

		link, err := uploadFileToDrive(driveService, tmpFile.Name(), fileName, objectFolderID)
		if err != nil {
			log.Printf("Ошибка загрузки файла на Google Drive: %v", err)
			continue
		}
		links = append(links, link)
		if len(links) >= 10 {
			break // Сохраним только первые 10 файлов
		}
	}

	if len(links) == 0 {
		reply := tgbotapi.NewMessage(message.Chat.ID, "Не удалось загрузить фотографии. Попробуйте снова.")
		bot.Send(reply)
		return
	}

	parsedData := ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  getFullName(message.From),
		Date:      time.Now().Format("02/01/2006 15:04:05"),
		DriveLink: strings.Join(links, " | "),
	}

	if err := appendToSheet(sheetsService, spreadsheetId, parsedData); err != nil {
		log.Printf("Ошибка записи в Google Sheets: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Ошибка записи данных. Попробуйте позже.")
		bot.Send(reply)
		return
	}

	reply := tgbotapi.NewMessage(message.Chat.ID, "Фотографии успешно добавлены.")
	bot.Send(reply)
}

func ensureObjectFolder(service *drive.Service, parentFolderId, objectName string) (string, error) {
	query := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and '%s' in parents", objectName, parentFolderId)
	fileList, err := service.Files.List().Q(query).Fields("files(id)").Do()
	if err != nil {
		return "", err
	}
	if len(fileList.Files) > 0 {
		return fileList.Files[0].Id, nil
	}

	folder := &drive.File{
		Name:     objectName,
		Parents:  []string{parentFolderId},
		MimeType: "application/vnd.google-apps.folder",
	}
	createdFolder, err := service.Files.Create(folder).Fields("id").Do()
	if err != nil {
		return "", err
	}
	return createdFolder.Id, nil
}

func main() {
	telegramToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN не установлен в переменных окружения")
	}

	spreadsheetId := os.Getenv("GOOGLE_SHEET_ID")
	if spreadsheetId == "" {
		log.Fatal("GOOGLE_SHEET_ID не установлен в переменных окружения")
	}

	driveFolderId := os.Getenv("GOOGLE_DRIVE_FOLDER_ID")
	if driveFolderId == "" {
		log.Fatal("GOOGLE_DRIVE_FOLDER_ID не установлен в переменных окружения")
	}

	adminIDStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	if adminIDStr == "" {
		log.Fatal("ADMIN_CHAT_ID не установлен в переменных окружения")
	}
	adminID, err := strconv.ParseInt(adminIDStr, 10, 64)
	if err != nil {
		log.Fatalf("Неверный формат ADMIN_CHAT_ID: %v", err)
	}

	googleClientID := os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret := os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	if googleClientID == "" || googleClientSecret == "" {
		log.Fatal("GOOGLE_OAUTH_CLIENT_ID и GOOGLE_OAUTH_CLIENT_SECRET должны быть установлены в переменных окружения")
	}

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

	client, err := getClient(oauthConfig)
	if err != nil {
		log.Fatalf("Не удалось получить OAuth2 клиента: %v", err)
	}

	sheetsService, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Не удалось создать Sheets сервис: %v", err)
	}
	driveService, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Не удалось создать Drive сервис: %v", err)
	}
	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Panic(err)
	}
	bot.Debug = true
	log.Printf("Авторизовался как %s", bot.Self.UserName)

	webhookURL := os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		log.Fatal("WEBHOOK_URL не установлен в переменных окружения")
	}

	url, err := url.Parse(webhookURL)
	if err != nil {
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}

	webhookConfig := tgbotapi.WebhookConfig{
		URL:            url,
		MaxConnections: 40,
	}
	_, err = bot.Request(webhookConfig)
	if err != nil {
		log.Fatalf("Не удалось установить Webhook: %v", err)
	}
	log.Printf("Webhook установлен на %s", webhookURL)
	keepAlive(webhookURL)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			bytes, _ := ioutil.ReadAll(r.Body)
			var update tgbotapi.Update
			err := json.Unmarshal(bytes, &update)
			if err != nil {
				log.Printf("Ошибка декодирования обновления: %v", err)
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}

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
			} else {
				semaphore <- struct{}{}
				wg.Add(1)
				go func(message *tgbotapi.Message) {
					defer wg.Done()
					handleMediaGroupMessage(bot, message, sheetsService, spreadsheetId, driveService, driveFolderId, adminID)
					<-semaphore
				}(update.Message)
			}

			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
		}
	})

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
