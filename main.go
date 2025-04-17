package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// Config хранит конфигурацию из переменных окружения
type Config struct {
	TelegramToken         string
	AdminChatID           int64
	SheetsID              string
	DriveFolderID         string
	WebhookURL            string
	Port                  string
	GoogleCredentialsJSON string
}

// loadConfig читает и валидирует необходимые переменные окружения
func loadConfig() (*Config, error) {
	var missing []string

	adminIDStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	if adminIDStr == "" {
		missing = append(missing, "ADMIN_CHAT_ID")
	}
	sheetID := strings.TrimSpace(os.Getenv("GOOGLE_SHEET_ID"))
	if sheetID == "" {
		missing = append(missing, "GOOGLE_SHEET_ID")
	}
	driveFolderID := strings.TrimSpace(os.Getenv("GOOGLE_DRIVE_FOLDER_ID"))
	if driveFolderID == "" {
		missing = append(missing, "GOOGLE_DRIVE_FOLDER_ID")
	}

	telegramToken := strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN"))
	if telegramToken == "" {
		missing = append(missing, "TELEGRAM_BOT_TOKEN")
	}

	webhookURL := strings.TrimSpace(os.Getenv("WEBHOOK_URL"))
	if webhookURL == "" {
		missing = append(missing, "WEBHOOK_URL")
	}

	credsJSON := strings.TrimSpace(os.Getenv("GOOGLE_CREDENTIALS_JSON"))
	if credsJSON == "" {
		missing = append(missing, "GOOGLE_CREDENTIALS_JSON")
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	adminID, err := strconv.ParseInt(adminIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ADMIN_CHAT_ID: %w", err)
	}

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}

	return &Config{
		TelegramToken:         telegramToken,
		AdminChatID:           adminID,
		SheetsID:              sheetID,
		DriveFolderID:         driveFolderID,
		WebhookURL:            webhookURL,
		Port:                  port,
		GoogleCredentialsJSON: credsJSON,
	}, nil
}

// регулярные выражения для парсинга подписи
var keywords = map[string]*regexp.Regexp{
	"address": regexp.MustCompile(`(?i)адрес[:\s-]*(.+)`),
	"amount":  regexp.MustCompile(`(?i)сумма[:\s-]*(.+)`),
	"comment": regexp.MustCompile(`(?i)комментари?й?[:\s-]*(.+)`),
}

// parseCaption извлекает адрес, сумму и комментарий из текста
func parseCaption(text string) (address, amount, comment string, err error) {
	for key, re := range keywords {
		if m := re.FindStringSubmatch(text); len(m) > 1 {
			switch key {
			case "address":
				address = strings.TrimSpace(m[1])
			case "amount":
				amount = cleanNumber(m[1])
			case "comment":
				comment = strings.TrimSpace(m[1])
			}
		}
	}
	if address == "" || amount == "" {
		return "", "", "", errors.New("не найдены обязательные поля: адрес или сумма")
	}
	return address, amount, comment, nil
}

// cleanNumber очищает строку до валидного числа
func cleanNumber(s string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	s = re.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, ",", ".")
	return s
}

// sanitizeFileName убирает недопустимые символы из имени файла
func sanitizeFileName(s string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9._-]`)
	return re.ReplaceAllString(s, "_")
}

// newGoogleServices инициализирует Sheets и Drive сервисы на основе JSON ключа сервисного аккаунта
func newGoogleServices(ctx context.Context, credsJSON string) (*sheets.Service, *drive.Service, error) {
	creds, err := google.CredentialsFromJSON(ctx, []byte(credsJSON),
		"https://www.googleapis.com/auth/spreadsheets",
		"https://www.googleapis.com/auth/drive.file",
	)
	if err != nil {
		return nil, nil, fmt.Errorf("credentials error: %w", err)
	}

	sheetsSrv, err := sheets.NewService(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("sheets service error: %w", err)
	}
	driveSrv, err := drive.NewService(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("drive service error: %w", err)
	}
	return sheetsSrv, driveSrv, nil
}

// uploadToDrive загружает поток на указанный Drive и возвращает webViewLink
func uploadToDrive(ctx context.Context, svc *drive.Service, folderID, fileName string, reader io.Reader) (string, error) {
	file := &drive.File{Name: fileName, Parents: []string{folderID}}
	res, err := svc.Files.Create(file).Media(reader).Fields("webViewLink").Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("drive upload error: %w", err)
	}
	return res.WebViewLink, nil
}

// appendToSheet добавляет строку в конец листа 'Чеки'
func appendToSheet(ctx context.Context, svc *sheets.Service, sheetID string, row []interface{}) error {
	rangeA1 := "'Чеки'!B:G"
	vr := &sheets.ValueRange{Values: [][]interface{}{row}}
	_, err := svc.Spreadsheets.Values.Append(sheetID, rangeA1, vr).
		ValueInputOption("USER_ENTERED").InsertDataOption("INSERT_ROWS").Context(ctx).
		Do()
	if err != nil {
		return fmt.Errorf("sheets append error: %w", err)
	}
	return nil
}

// handleUpdate обрабатывает одно обновление Telegram
func handleUpdate(ctx context.Context, bot *tgbotapi.BotAPI, sheetsSrv *sheets.Service, driveSrv *drive.Service, cfg *Config, update tgbotapi.Update) {
	msg := update.Message
	if msg == nil {
		return
	}

	if msg.IsCommand() {
		handleCommand(bot, cfg.AdminChatID, msg)
		return
	}

	if len(msg.Photo) == 0 || msg.Caption == "" {
		reply := tgbotapi.NewMessage(msg.Chat.ID, "📌 Отправьте фото чека с подписью: Адрес, Сумма, Комментарий (необязательно)")
		bot.Send(reply)
		return
	}

	address, amount, comment, err := parseCaption(msg.Caption)
	if err != nil {
		reply := tgbotapi.NewMessage(msg.Chat.ID, "❗ Ошибка формата подписи. Используйте /help для инструкции.")
		bot.Send(reply)
		return
	}

	file := msg.Photo[len(msg.Photo)-1]
	f, err := bot.GetFile(tgbotapi.FileConfig{FileID: file.FileID})
	if err != nil {
		log.Printf("get file error: %v", err)
		return
	}

	fileURL := f.Link(bot.Token)
	res, err := http.Get(fileURL)
	if err != nil {
		log.Printf("download photo error: %v", err)
		return
	}
	defer res.Body.Close()

	timestamp := time.Unix(int64(msg.Date), 0).Format("20060102_15-04-05")
	fileName := fmt.Sprintf("%s_%s.jpg", sanitizeFileName(address), timestamp)

	driveLink, err := uploadToDrive(ctx, driveSrv, cfg.DriveFolderID, fileName, res.Body)
	if err != nil {
		log.Printf("upload drive error: %v", err)
		return
	}

	row := []interface{}{time.Now().Format("02.01.2006 15:04:05"), getFullName(msg.From), address, amount, comment, driveLink}
	err = appendToSheet(ctx, sheetsSrv, cfg.SheetsID, row)
	if err != nil {
		log.Printf("append sheet error: %v", err)
		return
	}

	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "✅ Чек добавлен!"))
}

// handleCommand отвечает на команды /start и /help
func handleCommand(bot *tgbotapi.BotAPI, adminID int64, msg *tgbotapi.Message) {
	switch msg.Command() {
	case "start", "help":
		help := "🤖 Бот учёта чеков\n\n" +
			"Отправьте фото чека с подписью:\n" +
			"Адрес: ...\nСумма: ...\nКомментарий: ...(опционально)"
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, help))
	default:
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❓ Неизвестная команда. Используйте /help."))
	}
}

// getFullName возвращает полное имя пользователя
func getFullName(u *tgbotapi.User) string {
	if u == nil {
		return ""
	}
	if u.LastName != "" {
		return u.FirstName + " " + u.LastName
	}
	return u.FirstName
}

func main() {
	ctx := context.Background()
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	sheetsSrv, driveSrv, err := newGoogleServices(ctx, cfg.GoogleCredentialsJSON)
	if err != nil {
		log.Fatalf("google services error: %v", err)
	}

	bot, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		log.Fatalf("telegram bot error: %v", err)
	}
	bot.Debug = false

	// Настройка webhook
	hook, err := tgbotapi.NewWebhook(cfg.WebhookURL + "/" + bot.Token)
	if err != nil {
		log.Fatalf("new webhook error: %v", err)
	}
	hook.MaxConnections = 40

	_, err = bot.Request(hook)
	if err != nil {
		log.Fatalf("set webhook error: %v", err)
	}

	// Обработчик health
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Запуск HTTP-сервера
	server := &http.Server{Addr: ":" + cfg.Port}
	go func() {
		log.Printf("Listening on %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Канал обновлений
	updates := bot.ListenForWebhook("/" + bot.Token)

	// Обработка обновлений асинхронно
	var wg sync.WaitGroup
	for update := range updates {
		wg.Add(1)
		go func(u tgbotapi.Update) {
			defer wg.Done()
			handleUpdate(ctx, bot, sheetsSrv, driveSrv, cfg, u)
		}(update)
	}

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("Shutting down...")
	server.Shutdown(ctx)
	wg.Wait()
}
