package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"checkbot/config"
	"checkbot/internal/utils"
	"checkbot/pkg/logger"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// GoogleService структура для взаимодействия с Google API
type GoogleService struct {
	SheetsService *sheets.Service
	DriveService  *drive.Service
	Log           *logger.Logger
}

// NewGoogleService инициализирует Google сервисы (Sheets и Drive)
func NewGoogleService(cfg config.Config, logr *logger.Logger) (*GoogleService, error) {
	// Десериализация токена
	var token oauth2.Token
	err := json.Unmarshal([]byte(cfg.GoogleOAuthToken), &token)
	if err != nil {
		logr.Printf("Не удалось разобрать Google OAuth токен: %v", err)
		return nil, err
	}

	// Настройка OAuth2 конфигурации
	oauthConfig := &oauth2.Config{
		ClientID:     cfg.GoogleClientID,
		ClientSecret: cfg.GoogleClientSecret,
		RedirectURL:  cfg.WebhookURL,
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive.file",
		},
		Endpoint: google.Endpoint,
	}

	// Создание OAuth2 клиента
	client := oauthConfig.Client(context.Background(), &token)

	// Создание Google Sheets сервиса
	sheetsService, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		logr.Printf("Не удалось создать Sheets сервис: %v", err)
		return nil, err
	}

	// Создание Google Drive сервиса
	driveService, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		logr.Printf("Не удалось создать Drive сервис: %v", err)
		return nil, err
	}

	return &GoogleService{
		SheetsService: sheetsService,
		DriveService:  driveService,
		Log:           logr,
	}, nil
}

// UploadFile загружает файл на Google Drive и возвращает ссылку
func (gs *GoogleService) UploadFile(filePath, fileName, folderID string) (string, error) {
	// Открытие файла
	f, err := os.Open(filePath)
	if err != nil {
		gs.Log.Printf("Не удалось открыть файл: %v", err)
		return "", fmt.Errorf("не удалось открыть файл: %w", err)
	}
	defer f.Close()

	file := &drive.File{
		Name:    fileName,
		Parents: []string{folderID},
	}

	// Загрузка файла на Drive
	driveFile, err := gs.DriveService.Files.Create(file).Media(f).Fields("webViewLink").Do()
	if err != nil {
		gs.Log.Printf("Не удалось загрузить файл на Drive: %v", err)
		return "", fmt.Errorf("не удалось загрузить файл на Drive: %w", err)
	}

	return driveFile.WebViewLink, nil
}

// AppendToSheet добавляет данные в Google Sheets
func (gs *GoogleService) AppendToSheet(spreadsheetID string, data utils.ParsedData) error {
	values := []interface{}{
		data.Date,      // Столбец B
		data.Username,  // Столбец C
		data.Address,   // Столбец D
		data.Amount,    // Столбец E
		data.Comment,   // Столбец F
		data.DriveLink, // Столбец G
	}

	vr := &sheets.ValueRange{
		Values: [][]interface{}{values},
	}

	// Получение текущего количества строк
	resp, err := gs.SheetsService.Spreadsheets.Values.Get(spreadsheetID, "'Чеки'!B:B").Do()
	if err != nil {
		return fmt.Errorf("не удалось получить данные из Google Sheets: %w", err)
	}

	lastRow := 1
	if len(resp.Values) > 0 {
		lastRow = len(resp.Values) + 1
	}

	rangeStr := fmt.Sprintf("'Чеки'!B%d:G%d", lastRow, lastRow)

	_, err = gs.SheetsService.Spreadsheets.Values.Update(spreadsheetID, rangeStr, vr).
		ValueInputOption("RAW").
		Do()
	if err != nil {
		return fmt.Errorf("не удалось обновить Google Sheets: %w", err)
	}

	return nil
}
