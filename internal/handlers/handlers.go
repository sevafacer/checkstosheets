package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"checkbot/config"
	"checkbot/internal/services"
	"checkbot/internal/utils"
	"checkbot/pkg/logger"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// Handler структура обработчика
type Handler struct {
	TelegramService *services.TelegramService
	GoogleService   *services.GoogleService
	Config          config.Config
	Log             *logger.Logger
	semaphore       chan struct{}
	wg              sync.WaitGroup
}

// NewHandler создает новый обработчик
func NewHandler(ts *services.TelegramService, gs *services.GoogleService, cfg config.Config, logr *logger.Logger) *Handler {
	return &Handler{
		TelegramService: ts,
		GoogleService:   gs,
		Config:          cfg,
		Log:             logr,
		semaphore:       make(chan struct{}, 10), // Ограничение на 10 одновременных обработок
	}
}

// Run запускает обработчик
func (h *Handler) Run() error {
	// Настройка Webhook
	webhookURL := h.Config.WebhookURL
	parsedURL, err := url.Parse(webhookURL)
	if err != nil {
		h.Log.Printf("Некорректный формат WEBHOOK_URL: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Некорректный формат WEBHOOK_URL:* %v", err))
		return err
	}

	// Установка Webhook
	_, err = h.TelegramService.Bot.Request(tgbotapi.WebhookConfig{
		URL: parsedURL,
	})
	if err != nil {
		h.Log.Printf("Не удалось установить Webhook: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Не удалось установить Webhook:* %v", err))
		return err
	}
	h.Log.Printf("Webhook установлен на %s", webhookURL)

	// Настройка HTTP-сервера
	http.HandleFunc("/", h.handleUpdate)

	// Запуск сервера в отдельной горутине
	go func() {
		h.Log.Printf("Запуск HTTP сервера на %s", h.Config.ServerAddr)
		if err := http.ListenAndServe(h.Config.ServerAddr, nil); err != nil && err != http.ErrServerClosed {
			h.Log.Fatalf("Ошибка запуска HTTP сервера: %v", err)
		}
	}()

	// Ожидание сигнала завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	h.Log.Println("Получен сигнал завершения, останавливаем обработчик...")

	// Остановка Webhook
	_, err = h.TelegramService.Bot.Request(tgbotapi.DeleteWebhookConfig{
		DropPendingUpdates: true,
	})
	if err != nil {
		h.Log.Printf("Не удалось удалить Webhook: %v", err)
	} else {
		h.Log.Println("Webhook удален")
	}

	// Ожидание завершения горутин
	h.wg.Wait()

	h.Log.Println("Обработчик успешно остановлен.")
	return nil
}

// handleUpdate обрабатывает входящие обновления от Telegram
func (h *Handler) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
		return
	}

	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		h.Log.Printf("Ошибка декодирования обновления: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка декодирования обновления:* %v", err))
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if update.Message != nil && len(update.Message.Photo) > 0 {
		h.semaphore <- struct{}{}
		h.wg.Add(1)
		go h.handlePhotoMessage(context.Background(), update.Message)
	}

	w.WriteHeader(http.StatusOK)
}

// handlePhotoMessage обрабатывает сообщение с фотографией
func (h *Handler) handlePhotoMessage(ctx context.Context, message *tgbotapi.Message) {
	defer h.wg.Done()
	defer func() { <-h.semaphore }()

	comment := message.Caption

	address, amount, commentText, err := utils.ParseMessage(comment)
	if err != nil {
		h.Log.Printf("Ошибка при парсинге сообщения: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка парсинга сообщения от пользователя %s:* %v", utils.GetFullName(message.From), err))
		return
	}

	username := utils.GetFullName(message.From)
	dateFormatted := time.Unix(int64(message.Date), 0).Format("02012006_150405")

	if len(message.Photo) == 0 {
		h.Log.Println("Сообщение не содержит фотографий")
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Сообщение от пользователя %s не содержит фотографий*", username))
		return
	}

	// Выбор фото с наибольшим разрешением
	photo := message.Photo[len(message.Photo)-1]
	fileID := photo.FileID

	file, err := h.TelegramService.GetFile(fileID)
	if err != nil {
		h.Log.Printf("Ошибка при получении файла: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка при получении файла от пользователя %s:* %v", username, err))
		return
	}

	fileURL := file.Link(h.TelegramService.Bot.Token)
	resp, err := http.Get(fileURL)
	if err != nil {
		h.Log.Printf("Ошибка при скачивании файла: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка при скачивании файла от пользователя %s:* %v", username, err))
		return
	}
	defer resp.Body.Close()

	sanitizedAddress := utils.SanitizeFileName(address)
	fileName := fmt.Sprintf("%s_%s.jpg", sanitizedAddress, dateFormatted)

	tmpFile, err := os.CreateTemp("", fileName)
	if err != nil {
		h.Log.Printf("Не удалось создать временный файл: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Не удалось создать временный файл для пользователя %s:* %v", username, err))
		return
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		h.Log.Printf("Ошибка при сохранении файла: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка при сохранении файла для пользователя %s:* %v", username, err))
		return
	}

	driveLink, err := h.GoogleService.UploadFile(tmpFile.Name(), fileName, h.Config.DriveFolderID)
	if err != nil {
		h.Log.Printf("Ошибка при загрузке файла на Drive: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка при загрузке файла на Drive для пользователя %s:* %v", username, err))
		return
	}

	parsedData := utils.ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  username,
		Date:      time.Unix(int64(message.Date), 0).Format("02/01/2006 15:04:05"),
		DriveLink: driveLink,
	}

	if err := h.GoogleService.AppendToSheet(h.Config.SpreadsheetID, parsedData); err != nil {
		h.Log.Printf("Ошибка при записи в Google Sheets: %v", err)
		h.TelegramService.SendMessage(h.Config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка при записи в Google Sheets для пользователя %s:* %v", username, err))
		return
	}

	h.Log.Println("Данные успешно добавлены в Google Sheets и файл загружен на Drive")
}
