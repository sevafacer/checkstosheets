package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const (
	tokenRefreshWindow = 5 * time.Minute
	mediaGroupTTL      = 5 * time.Minute
	maxWorkers         = 50
)

type Services struct {
	bot        *tgbotapi.BotAPI
	drive      *drive.Service
	sheets     *sheets.Service
	oauth      *oauth2.Config
	mediaGroup *MediaGroupManager
	cfg        *Config
}

type MediaGroupManager struct {
	mu    sync.Mutex
	cache map[string]*MediaGroup
}

type MediaGroup struct {
	Files     []string
	Caption   string
	User      *tgbotapi.User
	CreatedAt time.Time
	Address   string
	Amount    string
	Comment   string
}

type Config struct {
	TelegramToken  string
	SpreadsheetID  string
	DriveFolderID  string
	AdminID        int64
	GoogleClientID string
	GoogleSecret   string
	WebhookURL     string
}

type Token struct {
	Access  string    `json:"access"`
	Refresh string    `json:"refresh"`
	Expiry  time.Time `json:"expiry"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := loadConfig()
	services := initServices(ctx, cfg)

	server := &http.Server{Addr: ":8080"}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleWebhook(services, w, r)
	})

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	keepAlive(cfg.WebhookURL)
	<-ctx.Done()
	server.Shutdown(ctx)
}

func loadConfig() *Config {
	return &Config{
		TelegramToken:  getEnv("TELEGRAM_BOT_TOKEN", true),
		SpreadsheetID:  getEnv("GOOGLE_SHEET_ID", true),
		DriveFolderID:  getEnv("GOOGLE_DRIVE_FOLDER_ID", true),
		AdminID:        parseInt(getEnv("ADMIN_CHAT_ID", true)),
		GoogleClientID: getEnv("GOOGLE_OAUTH_CLIENT_ID", true),
		GoogleSecret:   getEnv("GOOGLE_OAUTH_CLIENT_SECRET", true),
		WebhookURL:     getEnv("WEBHOOK_URL", true),
	}
}

func initServices(ctx context.Context, cfg *Config) *Services {
	oauth := &oauth2.Config{
		ClientID:     cfg.GoogleClientID,
		ClientSecret: cfg.GoogleSecret,
		RedirectURL:  cfg.WebhookURL,
		Scopes:       []string{sheets.SpreadsheetsScope, drive.DriveFileScope},
		Endpoint:     google.Endpoint,
	}

	return &Services{
		bot:        initBot(cfg.TelegramToken),
		drive:      initDriveService(ctx, getOAuthClient(ctx, oauth)),
		sheets:     initSheetsService(ctx, getOAuthClient(ctx, oauth)),
		oauth:      oauth,
		mediaGroup: &MediaGroupManager{cache: make(map[string]*MediaGroup)},
		cfg:        cfg,
	}
}

func handleWebhook(s *Services, w http.ResponseWriter, r *http.Request) {
	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		return
	}

	if update.Message != nil {
		s.HandleMessage(update.Message)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Services) HandleMessage(msg *tgbotapi.Message) {
	switch {
	case msg.IsCommand():
		s.handleCommand(msg)
	case msg.MediaGroupID != "":
		s.handleMediaGroup(msg)
	case len(msg.Photo) > 0:
		s.handleSinglePhoto(msg)
	}
}

func (s *Services) handleCommand(msg *tgbotapi.Message) {
	text := `📌 Отправьте фото чека с подписью в формате:
Адрес: [ваш адрес]
Сумма: [сумма]
Комментарий: [необязательно]`

	reply := tgbotapi.NewMessage(msg.Chat.ID, text)
	s.bot.Send(reply)
}

func (s *Services) handleMediaGroup(msg *tgbotapi.Message) {
	s.mediaGroup.mu.Lock()
	defer s.mediaGroup.mu.Unlock()

	groupID := msg.MediaGroupID
	group, exists := s.mediaGroup.cache[groupID]

	if !exists {
		group = &MediaGroup{
			User:      msg.From,
			CreatedAt: time.Now(),
		}
		s.mediaGroup.cache[groupID] = group
	}

	group.Files = append(group.Files, msg.Photo[len(msg.Photo)-1].FileID)
	group.parseCaption(msg.Caption)

	if time.Since(group.CreatedAt) > mediaGroupTTL || len(group.Files) >= 10 {
		go s.processMediaGroup(groupID, group)
		delete(s.mediaGroup.cache, groupID)
	}
}

func (s *Services) processMediaGroup(groupID string, group *MediaGroup) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if group.Address == "" || group.Amount == "" {
		s.sendUserResponse(group.User.ID, "❌ Отсутствует адрес или сумма")
		return
	}

	folderID := s.createObjectFolder(ctx, group.Address)
	links := s.uploadFiles(ctx, group.Files, folderID)

	if err := s.appendToSheet(ctx, group, links); err != nil {
		s.notifyAdmin(fmt.Sprintf("Sheet error: %v", err))
	}

	s.sendUserResponse(group.User.ID, fmt.Sprintf("✅ Обработано %d файлов", len(links)))
}

func (s *Services) createObjectFolder(ctx context.Context, name string) string {
	name = sanitizeName(name)
	folder := &drive.File{
		Name:     name,
		Parents:  []string{s.cfg.DriveFolderID},
		MimeType: "application/vnd.google-apps.folder",
	}

	result, err := s.drive.Files.Create(folder).Context(ctx).Do()
	if err != nil {
		log.Printf("Folder creation error: %v", err)
		return s.cfg.DriveFolderID
	}
	return result.Id
}

func (s *Services) uploadFiles(ctx context.Context, fileIDs []string, folderID string) []string {
	var wg sync.WaitGroup
	links := make([]string, 0, len(fileIDs))
	mu := sync.Mutex{}

	for _, fileID := range fileIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			url, _ := s.bot.GetFileDirectURL(id)
			link, err := s.uploadFile(ctx, url, folderID)
			if err == nil {
				mu.Lock()
				links = append(links, link)
				mu.Unlock()
			}
		}(fileID)
	}

	wg.Wait()
	return links
}

func (s *Services) uploadFile(ctx context.Context, url, folderID string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	file := &drive.File{
		Name:    fmt.Sprintf("%d", time.Now().UnixNano()),
		Parents: []string{folderID},
	}

	result, err := s.drive.Files.Create(file).Media(resp.Body).Context(ctx).Do()
	if err != nil {
		return "", err
	}
	return result.WebViewLink, nil
}

func (s *Services) appendToSheet(ctx context.Context, group *MediaGroup, links []string) error {
	values := &sheets.ValueRange{
		Values: [][]interface{}{{
			time.Now().Format("02.01.2006 15:04"),
			group.User.UserName,
			group.Address,
			strings.ReplaceAll(group.Amount, ",", "."),
			group.Comment,
			strings.Join(links, "\n"),
		}},
	}

	_, err := s.sheets.Spreadsheets.Values.Append(s.cfg.SpreadsheetID, "Чеки!A:F", values).
		ValueInputOption("USER_ENTERED").
		Context(ctx).
		Do()

	return err
}

func (s *Services) handleSinglePhoto(msg *tgbotapi.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Парсинг подписи
	group := &MediaGroup{
		User: msg.From,
	}
	group.parseCaption(msg.Caption)

	// Валидация обязательных полей
	if group.Address == "" || group.Amount == "" {
		s.sendUserResponse(msg.Chat.ID, "❌ В подписи отсутствует адрес или сумма")
		return
	}

	// Получение файла
	fileID := msg.Photo[len(msg.Photo)-1].FileID
	url, err := s.bot.GetFileDirectURL(fileID)
	if err != nil {
		s.sendUserResponse(msg.Chat.ID, "❌ Ошибка обработки файла")
		return
	}

	// Создание папки
	folderID := s.createObjectFolder(ctx, group.Address)

	// Загрузка файла
	link, err := s.uploadFile(ctx, url, folderID)
	if err != nil {
		log.Printf("Upload error: %v", err)
		s.sendUserResponse(msg.Chat.ID, "❌ Ошибка загрузки в Google Drive")
		return
	}

	// Запись в таблицу
	if err := s.appendToSheet(ctx, group, []string{link}); err != nil {
		log.Printf("Sheets error: %v", err)
		s.sendUserResponse(msg.Chat.ID, "❌ Ошибка записи данных")
		return
	}

	s.sendUserResponse(msg.Chat.ID, "✅ Фото успешно обработано")
}

// Дополним парсинг для разных форматов
func (mg *MediaGroup) parseCaption(text string) {
	// Удаляем лишние переносы строк
	text = strings.ReplaceAll(text, "\n", " ")

	// Основной regex с улучшенным паттерном
	re := regexp.MustCompile(`(?i)(адрес|объект)[:\s]*([^;]+?)\s*(?:;|$|сумма|коммент)|(сумма|оплата)[:\s]*([\d,]+)|(коммент|прим)[:\s]*([^;]*)`)
	matches := re.FindAllStringSubmatch(text, -1)

	for _, m := range matches {
		switch {
		case strings.EqualFold(m[1], "адрес") || strings.EqualFold(m[1], "объект"):
			mg.Address = strings.TrimSpace(m[2])
		case strings.EqualFold(m[3], "сумма") || strings.EqualFold(m[3], "оплата"):
			mg.Amount = strings.TrimSpace(m[4])
		case strings.EqualFold(m[5], "коммент") || strings.EqualFold(m[5], "прим"):
			mg.Comment = strings.TrimSpace(m[6])
		}
	}

	// Fallback для форматов без меток
	if mg.Address == "" || mg.Amount == "" {
		parts := strings.SplitN(text, " ", 3)
		if len(parts) >= 2 {
			mg.Address = parts[0]
			mg.Amount = parts[1]
			if len(parts) > 2 {
				mg.Comment = parts[2]
			}
		}
	}
}

func (s *Services) sendUserResponse(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeMarkdown
	s.bot.Send(msg)
}

func (s *Services) notifyAdmin(text string) {
	msg := tgbotapi.NewMessage(s.cfg.AdminID, text)
	s.bot.Send(msg)
}

func getOAuthClient(ctx context.Context, config *oauth2.Config) *http.Client {
	token := loadToken()
	if token != nil {
		tokenSource := config.TokenSource(ctx, &oauth2.Token{
			AccessToken:  token.Access,
			RefreshToken: token.Refresh,
			Expiry:       token.Expiry,
		})

		newToken, err := tokenSource.Token()
		if err == nil {
			saveToken(newToken)
			return oauth2.NewClient(ctx, tokenSource)
		}
	}

	newToken := requestNewToken(ctx, config)
	saveToken(newToken)
	return oauth2.NewClient(ctx, oauth2.StaticTokenSource(newToken))
}

func saveToken(token *oauth2.Token) {
	data, _ := json.Marshal(Token{
		Access:  token.AccessToken,
		Refresh: token.RefreshToken,
		Expiry:  token.Expiry,
	})
	os.Setenv("GOOGLE_TOKEN", base64.StdEncoding.EncodeToString(data))
}

func loadToken() *Token {
	data := os.Getenv("GOOGLE_TOKEN")
	if data == "" {
		return nil
	}

	decoded, _ := base64.StdEncoding.DecodeString(data)
	var token Token
	json.Unmarshal(decoded, &token)
	return &token
}

func requestNewToken(ctx context.Context, config *oauth2.Config) *oauth2.Token {
	url := config.AuthCodeURL("state", oauth2.AccessTypeOffline)
	fmt.Printf("Visit: %s\n", url)

	var code string
	fmt.Scanln(&code)

	token, _ := config.Exchange(ctx, code)
	return token
}

func initBot(token string) *tgbotapi.BotAPI {
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		log.Fatal(err)
	}
	bot.Debug = true
	return bot
}

func initDriveService(ctx context.Context, client *http.Client) *drive.Service {
	service, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatal(err)
	}
	return service
}

func initSheetsService(ctx context.Context, client *http.Client) *sheets.Service {
	service, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatal(err)
	}
	return service
}

func keepAlive(url string) {
	go func() {
		for range time.Tick(10 * time.Minute) {
			http.Get(url)
		}
	}()
}

func sanitizeName(name string) string {
	return regexp.MustCompile(`[^\w-]+`).ReplaceAllString(name, "_")
}

func getEnv(key string, required bool) string {
	val := os.Getenv(key)
	if required && val == "" {
		log.Fatalf("Missing required env var: %s", key)
	}
	return val
}

func parseInt(s string) int64 {
	val, _ := strconv.ParseInt(s, 10, 64)
	return val
}
