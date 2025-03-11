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

const (
	maxRetries         = 3
	retryDelay         = 2
	tokenRefreshWindow = 5 * time.Minute
	mediaGroupCacheTTL = 3 * time.Minute
	numWorkers         = 50
	sheetIDRange       = "'Чеки'!B:B"
	sheetUpdateRange   = "'Чеки'!B%d:G%d"
	sheetFormatRange   = "'Чеки'!B%d:G%d"
	sheetIDPropID      = 1051413829
)

var fieldKeywords = map[string][]string{
	"address": {"адрес", "объект", "квартира", "школа", "дом", "улица", "место", "локация"},
	"amount":  {"сумма", "стоимость", "оплата", "платёж", "цена"},
	"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение", "заметка"},
}

var tokenMutex sync.Mutex
var oauthConfig *oauth2.Config
var oauthState = "state-token"
var authCodeCh = make(chan string)
var mediaGroupCache = make(map[string]*MediaGroupData)
var mediaGroupCacheMu sync.Mutex
var mediaGroupExpiryCh = make(chan string, 100)
var taskQueue = make(chan FileTask, 100)
var resultsChan = make(chan FileResult, 100)
var semaphore = make(chan struct{}, 100)
var wg sync.WaitGroup

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
	Files           map[string]bool
	Caption         string
	Address         string
	Amount          string
	Comment         string
	LastUpdated     time.Time
	UserID          int64
	ChatID          int64
	Username        string
	ExpectedFiles   int
	ReceivedUpdates int
}

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

type fieldMatch struct {
	field string
	start int
	end   int
}

func loadEnvVars() (string, string, string, int64, string, string, string) {
	telegramToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN не установлен")
	}
	spreadsheetId := os.Getenv("GOOGLE_SHEET_ID")
	if spreadsheetId == "" {
		log.Fatal("GOOGLE_SHEET_ID не установлен")
	}
	driveFolderId := os.Getenv("GOOGLE_DRIVE_FOLDER_ID")
	if driveFolderId == "" {
		log.Fatal("GOOGLE_DRIVE_FOLDER_ID не установлен")
	}
	adminIDStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	if adminIDStr == "" {
		log.Fatal("ADMIN_CHAT_ID не установлен")
	}
	adminID, err := strconv.ParseInt(adminIDStr, 10, 64)
	if err != nil {
		log.Fatalf("Неверный формат ADMIN_CHAT_ID: %v", err)
	}
	googleClientID := os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret := os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	if googleClientID == "" || googleClientSecret == "" {
		log.Fatal("GOOGLE_OAUTH_CLIENT_ID и GOOGLE_OAUTH_CLIENT_SECRET не установлены")
	}
	webhookURL := os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		log.Fatal("WEBHOOK_URL не установлен")
	}
	return telegramToken, spreadsheetId, driveFolderId, adminID, googleClientID, googleClientSecret, webhookURL
}

func getOAuthClient(config *oauth2.Config) (*http.Client, error) {
	tokenMutex.Lock()
	token, err := loadTokenFromEnv()
	tokenMutex.Unlock()
	if err == nil {
		if time.Until(token.Expiry) < tokenRefreshWindow {
			if token.RefreshToken != "" {
				newToken, err := refreshToken(config, token)
				if err == nil {
					_ = saveTokenToEnv(newToken)
					return config.Client(context.Background(), newToken), nil
				}
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
		_ = server.Shutdown(ctx)
	}()
	authURL := config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	fmt.Printf("Перейдите по ссылке для авторизации:\n%v\n", authURL)
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
		return nil, errors.New("превышено время ожидания авторизации")
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
			http.Error(w, "Код не найден", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "Авторизация прошла успешно. Закройте окно.")
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

func uploadFileToDrive(service *drive.Service, filePath, fileName, folderId string) (string, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		f, err := os.Open(filePath)
		if err != nil {
			return "", fmt.Errorf("ошибка открытия файла: %v", err)
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
		service, _ = refreshDriveService(service, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return "", fmt.Errorf("загрузка файла не удалась: %v", lastErr)
}

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

func appendToSheet(service *sheets.Service, spreadsheetId string, data ParsedData) error {
	values := []interface{}{data.Date, data.Username, data.Address, data.Amount, data.Comment, data.DriveLink}
	vr := &sheets.ValueRange{Values: [][]interface{}{values}}
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
	return nil
}

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

func startMediaGroupCacheCleaner() {
	go func() {
		for id := range mediaGroupExpiryCh {
			mediaGroupCacheMu.Lock()
			delete(mediaGroupCache, id)
			mediaGroupCacheMu.Unlock()
		}
	}()
}

func startWorkers(num int, driveService *drive.Service, folderID string) {
	for i := 0; i < num; i++ {
		go worker(taskQueue, resultsChan, driveService, folderID)
	}
}

func worker(tasks <-chan FileTask, results chan<- FileResult, driveService *drive.Service, folderID string) {
	for task := range tasks {
		link, err := downloadAndUploadFile(task.FileURL, task.BaseName, task.DateFormatted, task.Amount, driveService, folderID)
		results <- FileResult{FileID: task.FileID, DriveLink: link, Error: err}
	}
}

func handleMediaGroupMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	if len(message.Photo) == 0 {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Сообщение не содержит фотографий."))
		return
	}
	bestPhoto := message.Photo[len(message.Photo)-1]
	moscowTime := time.Unix(int64(message.Date), 0).UTC().Add(3 * time.Hour)
	dateFormatted := moscowTime.Format("02.01.2006")
	if message.MediaGroupID != "" {
		mediaGroupCacheMu.Lock()
		groupData, exists := mediaGroupCache[message.MediaGroupID]
		if !exists {
			caption := message.Caption
			var addr, amt, comm string
			if caption != "" {
				var err error
				addr, amt, comm, err = parseMessage(caption)
				if err != nil {
					bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Не удалось распознать подпись."))
					mediaGroupCacheMu.Unlock()
					return
				}
			}
			groupData = &MediaGroupData{
				Files:           make(map[string]bool),
				Caption:         caption,
				Address:         addr,
				Amount:          amt,
				Comment:         comm,
				LastUpdated:     time.Now(),
				UserID:          message.From.ID,
				ChatID:          message.Chat.ID,
				Username:        getFullName(message.From),
				ExpectedFiles:   0,
				ReceivedUpdates: 0,
			}
			mediaGroupCache[message.MediaGroupID] = groupData
		}
		if groupData.ReceivedUpdates == 0 {
			groupData.ExpectedFiles = len(message.Photo)
		}
		groupData.ReceivedUpdates++
		if _, ok := groupData.Files[bestPhoto.FileID]; !ok {
			groupData.Files[bestPhoto.FileID] = false
			groupData.LastUpdated = time.Now()
		}
		if (groupData.ExpectedFiles > 0 && len(groupData.Files) >= groupData.ExpectedFiles) || time.Since(groupData.LastUpdated) > mediaGroupCacheTTL {
			go processMediaGroup(bot, message.MediaGroupID, sheetsService, spreadsheetId, driveService, parentFolderId, adminID)
			delete(mediaGroupCache, message.MediaGroupID)
		}
		mediaGroupCacheMu.Unlock()
		return
	}
	caption := message.Caption
	addr, amt, comm, err := parseMessage(caption)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Не удалось распознать подпись."))
		return
	}
	if addr == "" {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Введите адрес объекта."))
		return
	}
	if amt == "" {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Введите сумму."))
		return
	}
	folderID, err := ensureObjectFolder(driveService, parentFolderId, addr)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Ошибка обработки объекта."))
		return
	}
	sanitized := sanitizeFileName(addr)
	photoFile, err := bot.GetFile(tgbotapi.FileConfig{FileID: bestPhoto.FileID})
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Ошибка загрузки файла."))
		return
	}
	fileURL := photoFile.Link(bot.Token)
	link, err := downloadAndUploadFile(fileURL, sanitized, dateFormatted, amt, driveService, folderID)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Ошибка загрузки на Drive."))
		return
	}
	moscow := time.FixedZone("MSK", 3*3600)
	parsedData := ParsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  getFullName(message.From),
		Date:      time.Now().In(moscow).Format("02/01/2006 15:04:05"),
		DriveLink: link,
	}
	if err := appendToSheet(sheetsService, spreadsheetId, parsedData); err != nil {
		bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Ошибка записи данных."))
		return
	}
	bot.Send(tgbotapi.NewMessage(message.Chat.ID, "Фотография обработана."))
}

func processMediaGroup(bot *tgbotapi.BotAPI, mediaGroupID string, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, parentFolderId string, adminID int64) {
	time.Sleep(1 * time.Second)
	mediaGroupCacheMu.Lock()
	groupData, exists := mediaGroupCache[mediaGroupID]
	mediaGroupCacheMu.Unlock()
	if !exists || len(groupData.Files) == 0 {
		return
	}
	var unprocessed []string
	for id, processed := range groupData.Files {
		if !processed {
			unprocessed = append(unprocessed, id)
		}
	}
	if len(unprocessed) == 0 {
		return
	}
	addr, amt, comm, chatID, username := groupData.Address, groupData.Amount, groupData.Comment, groupData.ChatID, groupData.Username
	if addr == "" || amt == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "Введите адрес и сумму."))
		return
	}
	folderID, err := ensureObjectFolder(driveService, parentFolderId, addr)
	if err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "Ошибка обработки объекта."))
		return
	}
	moscow := time.FixedZone("MSK", 3*3600)
	dateFormatted := time.Now().In(moscow).Format("02.01.2006")
	sanitized := sanitizeFileName(addr)
	var tasks []FileTask
	for i, fileID := range unprocessed {
		photoFile, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
		if err != nil {
			continue
		}
		fileURL := photoFile.Link(bot.Token)
		name := fmt.Sprintf("%s_%d", sanitized, i+1)
		tasks = append(tasks, FileTask{
			FileID:         fileID,
			FileURL:        fileURL,
			BaseName:       name,
			DateFormatted:  dateFormatted,
			Amount:         amt,
			DriveService:   driveService,
			ObjectFolderID: folderID,
		})
	}
	if len(tasks) == 0 {
		return
	}
	startWorkers(numWorkers, driveService, folderID)
	go func() {
		for _, task := range tasks {
			taskQueue <- task
		}
		close(taskQueue)
	}()
	var links []string
	processedFiles := make(map[string]bool)
	for range tasks {
		result := <-resultsChan
		if result.Error == nil {
			links = append(links, result.DriveLink)
			processedFiles[result.FileID] = true
		}
	}
	close(resultsChan)
	mediaGroupCacheMu.Lock()
	if group, ok := mediaGroupCache[mediaGroupID]; ok {
		for id := range processedFiles {
			group.Files[id] = true
		}
	}
	mediaGroupCacheMu.Unlock()
	if len(links) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Не удалось загрузить фотографии."))
		return
	}
	parsedData := ParsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  username,
		Date:      time.Now().In(moscow).Format("02/01/2006 15:04:05"),
		DriveLink: strings.Join(links, " "),
	}
	go func() {
		if err := appendToSheet(sheetsService, spreadsheetId, parsedData); err != nil {
			notifyAdminAboutSheetError(bot, adminID, err, mediaGroupID)
		}
	}()
	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Обработано %d фото.", len(links))))
}

func notifyAdminAboutSheetError(bot *tgbotapi.BotAPI, adminID int64, err error, mediaGroupID string) {
	msg := tgbotapi.NewMessage(adminID, fmt.Sprintf("Ошибка Sheets для медиагруппы %s: %v", mediaGroupID, err))
	_, _ = bot.Send(msg)
}

func downloadAndUploadFile(fileURL, baseName, dateFormatted, amount string, driveService *drive.Service, folderID string) (string, error) {
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("ошибка скачивания: %v", err)
	}
	defer resp.Body.Close()
	fileName := sanitizeFileName(fmt.Sprintf("%s_%s_%s.jpg", baseName, dateFormatted, amount))
	tmpFile, err := os.CreateTemp("", fileName)
	if err != nil {
		return "", fmt.Errorf("ошибка создания temp файла: %v", err)
	}
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", fmt.Errorf("ошибка копирования: %v", err)
	}
	tmpFile.Close()
	link, err := uploadFileToDrive(driveService, tmpFile.Name(), fileName, folderID)
	if err != nil {
		return "", fmt.Errorf("ошибка загрузки на Drive: %v", err)
	}
	return link, nil
}

func getFullName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

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

func cleanAmount(amount string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	cleaned := re.ReplaceAllString(amount, "")
	return strings.ReplaceAll(cleaned, ".", ",")
}

func sanitizeFileName(name string) string {
	re := regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s\.-]`)
	sanitized := re.ReplaceAllString(name, "_")
	sanitized = regexp.MustCompile(`\s+`).ReplaceAllString(sanitized, " ")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	sanitized = regexp.MustCompile(`_+`).ReplaceAllString(sanitized, "_")
	return strings.Trim(sanitized, "_")
}

func main() {
	telegramToken, spreadsheetId, driveFolderId, adminID, googleClientID, googleClientSecret, webhookURL := loadEnvVars()
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
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			_, err := getOAuthClient(oauthConfig)
			if err != nil {
			}
		}
	}()
	client, err := getOAuthClient(oauthConfig)
	if err != nil {
		log.Fatalf("OAuth клиент не получен: %v", err)
	}
	sheetsService, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Sheets сервис не создан: %v", err)
	}
	driveService, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Drive сервис не создан: %v", err)
	}
	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Panic(err)
	}
	bot.Debug = true
	parsedWebhookURL, err := url.Parse(webhookURL)
	if err != nil {
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}
	webhookConfig := tgbotapi.WebhookConfig{URL: parsedWebhookURL, MaxConnections: 40}
	_, err = bot.Request(webhookConfig)
	if err != nil {
		log.Fatalf("Webhook не установлен: %v", err)
	}
	keepAlive(webhookURL)
	startMediaGroupCacheCleaner()
	startWorkers(numWorkers, driveService, driveFolderId)
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
				if update.Message.IsCommand() {
					switch update.Message.Command() {
					case "start", "help":
						helpText := `Привет! Я бот для отслеживания чеков.
Пришли фотографию чека с подписью:
Адрес: [адрес]
Сумма: [сумма]
Комментарий: [комментарий (опционально)]`
						bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, helpText))
					}
				} else if update.Message.Photo != nil || update.Message.MediaGroupID != "" {
					semaphore <- struct{}{}
					wg.Add(1)
					go func(m *tgbotapi.Message) {
						defer wg.Done()
						handleMediaGroupMessage(bot, m, sheetsService, spreadsheetId, driveService, driveFolderId, adminID)
						<-semaphore
					}(update.Message)
				}
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
	server := &http.Server{Addr: ":" + port}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP сервер не запущен: %v", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	wg.Wait()
}
