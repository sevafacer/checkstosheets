package services

import (
	"checkbot/config"
	"checkbot/pkg/logger"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// TelegramService структура для взаимодействия с Telegram API
type TelegramService struct {
	Bot *tgbotapi.BotAPI
	Log *logger.Logger
}

// NewTelegramService инициализирует Telegram сервис
func NewTelegramService(cfg config.Config, logr *logger.Logger) (*TelegramService, error) {
	bot, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		logr.Printf("Не удалось создать Telegram бота: %v", err)
		return nil, err
	}
	bot.Debug = false // В продакшн режиме лучше отключить отладку
	logr.Printf("Авторизовался как %s", bot.Self.UserName)

	return &TelegramService{
		Bot: bot,
		Log: logr,
	}, nil
}

// SendMessage отправляет сообщение в указанный чат
func (ts *TelegramService) SendMessage(chatID int64, message string) error {
	msg := tgbotapi.NewMessage(chatID, message)
	_, err := ts.Bot.Send(msg)
	if err != nil {
		ts.Log.Printf("Ошибка при отправке сообщения: %v", err)
		return err
	}
	return nil
}

// GetFile получает информацию о файле по его ID
func (ts *TelegramService) GetFile(fileID string) (*tgbotapi.File, error) {
	fileConfig := tgbotapi.FileConfig{FileID: fileID}
	file, err := ts.Bot.GetFile(fileConfig)
	if err != nil {
		ts.Log.Printf("Ошибка при получении файла: %v", err)
		return nil, err
	}
	return &file, nil // Возвращаем указатель на файл
}
