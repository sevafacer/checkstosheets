package main

import (
	"checkbot/config"
	"checkbot/internal/handlers"
	"checkbot/internal/services"
	"checkbot/pkg/logger"
)

func main() {
	// Инициализация логгера
	logr := logger.NewLogger()

	// Загрузка конфигурации
	cfg, err := config.LoadConfig()
	if err != nil {
		logr.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	// Инициализация сервисов
	telegramService, err := services.NewTelegramService(cfg, logr)
	if err != nil {
		logr.Fatalf("Ошибка инициализации Telegram сервиса: %v", err)
	}

	googleService, err := services.NewGoogleService(cfg, logr)
	if err != nil {
		logr.Fatalf("Ошибка инициализации Google сервиса: %v", err)
	}

	// Инициализация обработчиков
	handler := handlers.NewHandler(telegramService, googleService, cfg, logr)

	// Запуск сервера
	if err := handler.Run(); err != nil {
		logr.Fatalf("Ошибка запуска сервера: %v", err)
	}
}
