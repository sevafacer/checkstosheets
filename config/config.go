package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config структура для хранения конфигурации приложения
type Config struct {
	TelegramToken      string
	SpreadsheetID      string
	DriveFolderID      string
	GoogleClientID     string
	GoogleClientSecret string
	ServerAddr         string
	WebhookURL         string
	Port               string
	DeveloperChatID    int64
	GoogleOAuthToken   string
}

// LoadConfig загружает конфигурацию из переменных окружения
func LoadConfig() (Config, error) {
	cfg := Config{
		TelegramToken:      os.Getenv("TELEGRAM_BOT_TOKEN"),
		SpreadsheetID:      os.Getenv("GOOGLE_SHEET_ID"),
		DriveFolderID:      os.Getenv("GOOGLE_DRIVE_FOLDER_ID"),
		GoogleClientID:     os.Getenv("GOOGLE_OAUTH_CLIENT_ID"),
		GoogleClientSecret: os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET"),
		ServerAddr:         os.Getenv("SERVER_ADDR"),
		WebhookURL:         os.Getenv("WEBHOOK_URL"),
		Port:               os.Getenv("PORT"),
		GoogleOAuthToken:   os.Getenv("GOOGLE_OAUTH_TOKEN"),
	}

	// Конвертация DeveloperChatID из строки в int64
	developerChatIDStr := os.Getenv("DEVELOPER_CHAT_ID")
	if developerChatIDStr != "" {
		id, err := strconv.ParseInt(developerChatIDStr, 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("некорректный формат DEVELOPER_CHAT_ID: %v", err)
		}
		cfg.DeveloperChatID = id
	}

	// Установка значений по умолчанию
	if cfg.ServerAddr == "" {
		cfg.ServerAddr = ":8080"
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}

	// Проверка обязательных полей
	missing := []string{}
	if cfg.TelegramToken == "" {
		missing = append(missing, "TELEGRAM_BOT_TOKEN")
	}
	if cfg.SpreadsheetID == "" {
		missing = append(missing, "GOOGLE_SHEET_ID")
	}
	if cfg.DriveFolderID == "" {
		missing = append(missing, "GOOGLE_DRIVE_FOLDER_ID")
	}
	if cfg.GoogleClientID == "" || cfg.GoogleClientSecret == "" {
		missing = append(missing, "GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET")
	}
	if cfg.WebhookURL == "" {
		missing = append(missing, "WEBHOOK_URL")
	}
	if cfg.DeveloperChatID == 0 {
		missing = append(missing, "DEVELOPER_CHAT_ID")
	}
	if cfg.GoogleOAuthToken == "" {
		missing = append(missing, "GOOGLE_OAUTH_TOKEN")
	}

	if len(missing) > 0 {
		return cfg, fmt.Errorf("отсутствуют обязательные переменные окружения: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}
