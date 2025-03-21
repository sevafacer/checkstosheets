package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	maxRetries = 3
	retryDelay = 2
)

var (
	telegramToken, sheetID, driveFolderID string
	adminID                               int64
	webhookURL                            string

	googleClientID, googleClientSecret string
	oauthConfig                        *oauth2.Config
	tokenFile                          = "token.json"

	sheetsSrv *sheets.Service
	driveSrv  *drive.Service

	tokenMutex           sync.Mutex
	MaxConcurrentUploads = 5
)

type ParsedData struct {
	Address, Amount, Comment, Username, Date, DriveLink string
}

func LoadEnvVars() (string, string, string, int64, string) {
	telegramToken = os.Getenv("TELEGRAM_BOT_TOKEN")
	sheetID = os.Getenv("GOOGLE_SHEET_ID")
	driveFolderID = os.Getenv("GOOGLE_DRIVE_FOLDER_ID")
	adminStr := os.Getenv("ADMIN_CHAT_ID")
	googleClientID = os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret = os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	webhookURL = os.Getenv("WEBHOOK_URL")
	if telegramToken == "" || sheetID == "" || driveFolderID == "" || adminStr == "" || googleClientID == "" || googleClientSecret == "" || webhookURL == "" {
		log.Fatal("Одна или несколько обязательных переменных окружения не установлены")
	}
	var err error
	adminStr = strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	adminID, err = strconv.ParseInt(adminStr, 10, 64)
	if err != nil {
		log.Fatalf("Неверный формат ADMIN_CHAT_ID: %v", err)
	}
	return telegramToken, sheetID, driveFolderID, adminID, webhookURL
}

func AdminID() int64 {
	return adminID
}

func Initialize(sheet string, driveFolder string) {
	sheetID = sheet
	driveFolderID = driveFolder
	oauthConfig = &oauth2.Config{
		ClientID:     googleClientID,
		ClientSecret: googleClientSecret,
		RedirectURL:  "http://localhost",
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive.file",
		},
		Endpoint: google.Endpoint,
	}
	client, err := getOAuthClient(oauthConfig)
	if err != nil {
		log.Fatalf("OAuth клиент не получен: %v", err)
	}
	sheetsSrv, err = sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Sheets сервис не создан: %v", err)
	}
	driveSrv, err = drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Drive сервис не создан: %v", err)
	}
	go tokenRefreshRoutine()
}

func tokenRefreshRoutine() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		_, _ = getOAuthClient(oauthConfig)
	}
}

func getOAuthClient(config *oauth2.Config) (*http.Client, error) {
	tokenMutex.Lock()
	token, err := loadTokenFromFile()
	tokenMutex.Unlock()
	if err == nil && token.Valid() {
		return config.Client(context.Background(), token), nil
	}
	log.Fatal("Токен OAuth не найден или недействителен. Требуется авторизация.")
	return nil, errors.New("токен OAuth не найден")
}

func saveTokenToFile(token *oauth2.Token) error {
	data, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга токена: %v", err)
	}
	return os.WriteFile(tokenFile, data, 0600)
}

func loadTokenFromFile() (*oauth2.Token, error) {
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}
	var token oauth2.Token
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, err
	}
	return &token, nil
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

func EnsureObjectFolder(address string) (string, string, error) {
	folderName := SanitizeFileName(strings.TrimSpace(address))
	if folderName == "" {
		folderName = "Разное"
	}
	query := fmt.Sprintf("name='%s' and mimeType='application/vnd.google-apps.folder' and '%s' in parents and trashed=false", folderName, driveFolderID)
	var fileList *drive.FileList
	var err error
	for i := 0; i < maxRetries; i++ {
		fileList, err = driveSrv.Files.List().Q(query).Fields("files(id)").PageSize(10).Do()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	if err != nil {
		return "", "", fmt.Errorf("поиск папки не удался: %v", err)
	}
	if len(fileList.Files) > 0 {
		return fileList.Files[0].Id, "Чек загружен в существующую папку", nil
	}
	folder := &drive.File{
		Name:     folderName,
		Parents:  []string{driveFolderID},
		MimeType: "application/vnd.google-apps.folder",
	}
	var created *drive.File
	for i := 0; i < maxRetries; i++ {
		created, err = driveSrv.Files.Create(folder).Fields("id").Do()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	if err != nil {
		return "", "", fmt.Errorf("создание папки не удалось: %v", err)
	}
	return created.Id, "Создана новая папка и чек загружен туда", nil
}

func UploadPhoto(fileURL, folderID, fileName string) (string, error) {
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("ошибка скачивания: %v", err)
	}
	defer resp.Body.Close()
	tmpFile, err := os.CreateTemp("", "tg_photo_*")
	if err != nil {
		return "", fmt.Errorf("ошибка создания temp файла: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("ошибка копирования: %v", err)
	}
	tmpFile.Close()
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		file, err := os.Open(tmpFile.Name())
		if err != nil {
			return "", fmt.Errorf("ошибка открытия файла: %v", err)
		}
		driveFile := &drive.File{
			Name:    fileName,
			Parents: []string{folderID},
		}
		res, err := driveSrv.Files.Create(driveFile).Media(file).Fields("webViewLink").Do()
		file.Close()
		if err == nil {
			return res.WebViewLink, nil
		}
		lastErr = err
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return "", fmt.Errorf("загрузка файла не удалась после %d попыток: %v", maxRetries, lastErr)
}

func DownloadAndUploadFile(fileURL, fileName, folderID string) (string, error) {
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	driveFile := &drive.File{
		Name:     fileName,
		MimeType: "image/jpeg",
		Parents:  []string{folderID},
	}
	file, err := driveSrv.Files.Create(driveFile).Media(resp.Body).Do()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("https://drive.google.com/file/d/%s/view", file.Id), nil
}

func AppendToSheet(data ParsedData) error {
	values := []interface{}{data.Date, data.Username, data.Address, data.Amount, data.Comment, data.DriveLink}
	vr := &sheets.ValueRange{Values: [][]interface{}{values}}
	resp, err := sheetsSrv.Spreadsheets.Values.Get(sheetID, "'Чеки'!B:B").Do()
	if err != nil {
		return fmt.Errorf("получение данных не удалось: %v", err)
	}
	row := len(resp.Values) + 1
	_, err = sheetsSrv.Spreadsheets.Values.Update(sheetID, fmt.Sprintf("'Чеки'!B%d:G%d", row, row), vr).ValueInputOption("USER_ENTERED").Do()
	return err
}

func NotifyAdminSuccess(bot *tgbotapi.BotAPI, adminID int64, data ParsedData, userMsg *tgbotapi.Message, folderMsg string) {
	var origMsg string
	if userMsg != nil {
		origMsg = userMsg.Text
	}
	msgText := fmt.Sprintf("✅ Чек успешно загружен!\n\nПользователь: %s\nВремя: %s\nОбъект: %s\nСумма: %s\nКомментарий: %s\nСсылка на файл: %s\nПапка: %s\n\nИсходное сообщение:\n%s", data.Username, data.Date, data.Address, data.Amount, data.Comment, data.DriveLink, folderMsg, origMsg)
	adminMsg := tgbotapi.NewMessage(adminID, msgText)
	bot.Send(adminMsg)
}

func NotifyAdminFailure(bot *tgbotapi.BotAPI, adminID int64, err error, userMsg *tgbotapi.Message) {
	var origMsg, userName string
	if userMsg != nil {
		origMsg = userMsg.Text
		userName = getFullName(userMsg.From)
	}
	msgText := fmt.Sprintf("❗️ Ошибка обработки чека!\n\nПользователь: %s\nИсходное сообщение:\n%s\n\nОшибка: %v", userName, origMsg, err)
	adminMsg := tgbotapi.NewMessage(adminID, msgText)
	bot.Send(adminMsg)
}

func SanitizeFileName(name string) string {
	re := regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s\.-]`)
	s := re.ReplaceAllString(name, "_")
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, "_")
	return strings.Trim(regexp.MustCompile(`_+`).ReplaceAllString(s, "_"), "_")
}

func getFullName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}
