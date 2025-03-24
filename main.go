package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// Константы
const (
	tokenFile          = "token.json"
	sheetRange         = "'Чеки'!B:B"
	sheetUpdateFormat  = "'Чеки'!B%d:G%d"
	maxRetries         = 3
	retryDelay         = 2 * time.Second
	maxConcurrent      = 5
	tokenRefreshWindow = 5 * time.Minute
)

// Глобальные переменные
var (
	mediaCache    = make(map[string]*MediaGroup)
	mediaCacheMu  sync.Mutex
	tokenMu       sync.Mutex
	authCodeCh    = make(chan string, 1)
	oauthState    = "state-token"
	fieldKeywords = map[string][]string{
		"address": {"адрес", "объект", "квартира", "школа", "дом", "улица", "место", "локация"},
		"amount":  {"сумма", "стоимость", "оплата", "платёж", "цена"},
		"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение", "заметка"},
	}
	objects = []string{
		"Тимирязева 19, кв. 201",
		"Каскад 2",
		"Каскад 1",
	}
)

// Структуры
type Config struct {
	TelegramToken string
	SheetID       string
	DriveFolderID string
	AdminID       int64
	ClientID      string
	ClientSecret  string
	WebhookURL    string
}

type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string
}

type MediaGroup struct {
	Photos     map[string]*tgbotapi.PhotoSize
	Address    string
	Amount     string
	Comment    string
	Username   string
	ChatID     int64
	UpdatedAt  time.Time
	Processing bool
}

// Конфигурация
func loadConfig() Config {
	cfg := Config{
		TelegramToken: os.Getenv("TELEGRAM_BOT_TOKEN"),
		SheetID:       os.Getenv("GOOGLE_SHEET_ID"),
		DriveFolderID: os.Getenv("GOOGLE_DRIVE_FOLDER_ID"),
		ClientID:      os.Getenv("GOOGLE_OAUTH_CLIENT_ID"),
		ClientSecret:  os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET"),
		WebhookURL:    os.Getenv("WEBHOOK_URL"),
	}
	adminID, err := strconv.ParseInt(strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID")), 10, 64)
	if err != nil {
		log.Fatalf("Invalid ADMIN_CHAT_ID: %v", err)
	}
	cfg.AdminID = adminID

	for _, v := range []string{cfg.TelegramToken, cfg.SheetID, cfg.DriveFolderID, cfg.ClientID, cfg.ClientSecret, cfg.WebhookURL} {
		if v == "" {
			log.Fatal("Missing required environment variable")
		}
	}
	return cfg
}

// OAuth
func initOAuth(cfg Config) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  "http://localhost:8080",
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive.file",
		},
		Endpoint: google.Endpoint,
	}
}

func getClient(oauthCfg *oauth2.Config) *http.Client {
	tokenMu.Lock()
	defer tokenMu.Unlock()

	token, err := loadToken()
	if err == nil && token.Valid() {
		return oauthCfg.Client(context.Background(), token)
	}

	return authenticate(oauthCfg)
}

func loadToken() (*oauth2.Token, error) {
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}
	var token oauth2.Token
	return &token, json.Unmarshal(data, &token)
}

func saveToken(token *oauth2.Token) error {
	data, err := json.Marshal(token)
	if err != nil {
		return err
	}
	return os.WriteFile(tokenFile, data, 0600)
}

func authenticate(oauthCfg *oauth2.Config) *http.Client {
	errCh := make(chan error, 1)
	srv := startAuthServer(errCh)
	defer srv.Shutdown(context.Background())

	authURL := oauthCfg.AuthCodeURL(oauthState, oauth2.AccessTypeOffline)
	log.Printf("Authorize here: %s", authURL)

	select {
	case code := <-authCodeCh:
		token, err := oauthCfg.Exchange(context.Background(), code)
		if err != nil {
			log.Fatalf("OAuth exchange failed: %v", err)
		}
		if err := saveToken(token); err != nil {
			log.Printf("Failed to save token: %v", err)
		}
		return oauthCfg.Client(context.Background(), token)
	case err := <-errCh:
		log.Fatalf("Auth server error: %v", err)
	case <-time.After(5 * time.Minute):
		log.Fatal("Authentication timeout")
	}
	return nil
}

func startAuthServer(errCh chan error) *http.Server {
	srv := &http.Server{Addr: ":8080"}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != oauthState {
			http.Error(w, "Invalid state", http.StatusBadRequest)
			return
		}
		if code := r.URL.Query().Get("code"); code != "" {
			fmt.Fprintln(w, "Authorization successful. You can close this window.")
			authCodeCh <- code
		} else {
			http.Error(w, "Code missing", http.StatusBadRequest)
		}
	})
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	return srv
}

// Парсинг
func parseCaption(caption string) (string, string, string, error) {
	if caption == "" {
		return "", "", "", errors.New("empty caption")
	}
	lines := strings.Split(strings.TrimSpace(caption), "\n")
	if len(lines) >= 2 {
		return cleanAddress(lines[0]), cleanAmount(lines[1]), strings.Join(lines[2:], " "), nil
	}
	return "", "", "", errors.New("invalid format")
}

func cleanAddress(addr string) string {
	return strings.TrimSpace(removeKeywords(addr, fieldKeywords["address"]))
}

func cleanAmount(amt string) string {
	amt = removeKeywords(amt, fieldKeywords["amount"])
	re := regexp.MustCompile(`[^0-9.,]`)
	return strings.ReplaceAll(re.ReplaceAllString(amt, ""), ".", ",")
}

func removeKeywords(text string, keywords []string) string {
	for _, kw := range keywords {
		if strings.HasPrefix(strings.ToLower(text), kw) {
			return strings.TrimSpace(text[len(kw):])
		}
	}
	return text
}

// Google Drive
func ensureFolder(driveSrv *drive.Service, parentID, name string) (string, string, error) {
	name = sanitizeName(name)
	if name == "" {
		name = "Miscellaneous"
	}
	folderID, err := findFolder(driveSrv, parentID, name)
	if err == nil && folderID != "" {
		return folderID, "Uploaded to existing folder", nil
	}
	return createFolder(driveSrv, parentID, name)
}

func findFolder(driveSrv *drive.Service, parentID, name string) (string, error) {
	query := fmt.Sprintf("name='%s' '%s' in parents mimeType='application/vnd.google-apps.folder' trashed=false", name, parentID)
	r, err := driveSrv.Files.List().Q(query).Fields("files(id)").Do()
	if err != nil {
		return "", err
	}
	if len(r.Files) > 0 {
		return r.Files[0].Id, nil
	}
	return "", nil
}

func createFolder(driveSrv *drive.Service, parentID, name string) (string, string, error) {
	folder := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{parentID},
	}
	f, err := driveSrv.Files.Create(folder).Fields("id").Do()
	if err != nil {
		return "", "", err
	}
	return f.Id, "Created new folder", nil
}

func uploadPhotoToDrive(bot *tgbotapi.BotAPI, driveSrv *drive.Service, fileID, folderID, addr, amt string, index int) (string, error) {
	fileURL, err := bot.GetFileDirectURL(fileID)
	if err != nil {
		return "", err
	}
	data, err := downloadFile(fileURL)
	if err != nil {
		return "", err
	}
	defer os.Remove(data.Name())
	return uploadFile(driveSrv, data, folderID, generateFileName(addr, amt, index))
}

func downloadFile(url string) (*os.File, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	f, err := os.CreateTemp("", "photo_*.jpg")
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(f, resp.Body)
	return f, err
}

func uploadFile(driveSrv *drive.Service, file *os.File, folderID, name string) (string, error) {
	f, err := driveSrv.Files.Create(&drive.File{
		Name:    name,
		Parents: []string{folderID},
	}).Media(file).Fields("webViewLink").Do()
	if err != nil {
		return "", err
	}
	return f.WebViewLink, nil
}

func sanitizeName(name string) string {
	re := regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s\.-]`)
	return strings.Trim(regexp.MustCompile(`_+`).ReplaceAllString(regexp.MustCompile(`\s+`).ReplaceAllString(re.ReplaceAllString(name, "_"), "_"), "_"), "_")
}

func generateFileName(addr, amt string, index int) string {
	msk := time.FixedZone("MSK", 3*3600)
	return fmt.Sprintf("%s_%s_%s_%02d.jpg", time.Now().In(msk).Format("020106"), sanitizeName(addr), sanitizeName(amt), index)
}

// Google Sheets
func appendData(sheetsSrv *sheets.Service, sheetID string, data ParsedData) error {
	resp, err := sheetsSrv.Spreadsheets.Values.Get(sheetID, sheetRange).Do()
	if err != nil {
		return err
	}
	row := len(resp.Values) + 1
	values := [][]interface{}{{data.Date, data.Username, data.Address, data.Amount, data.Comment, data.DriveLink}}
	_, err = sheetsSrv.Spreadsheets.Values.Update(sheetID, fmt.Sprintf(sheetUpdateFormat, row, row), &sheets.ValueRange{Values: values}).ValueInputOption("USER_ENTERED").Do()
	return err
}

// Telegram
func processSinglePhoto(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, driveSrv *drive.Service, sheetsSrv *sheets.Service, cfg Config) {
	addr, amt, comm, err := parseCaption(msg.Caption)
	if err != nil {
		sendError(bot, msg.Chat.ID, cfg.AdminID, "Invalid caption format", msg)
		return
	}
	folderID, folderMsg, err := ensureFolder(driveSrv, cfg.DriveFolderID, addr)
	if err != nil {
		sendError(bot, msg.Chat.ID, cfg.AdminID, fmt.Sprintf("Folder error: %v", err), msg)
		return
	}
	best := msg.Photo[len(msg.Photo)-1]
	link, err := uploadPhotoToDrive(bot, driveSrv, best.FileID, folderID, addr, amt, 1)
	if err != nil {
		sendError(bot, msg.Chat.ID, cfg.AdminID, fmt.Sprintf("Upload failed: %v", err), msg)
		return
	}
	data := ParsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  getUserName(msg.From),
		Date:      time.Now().Format("02.01.2006 15:04:05"),
		DriveLink: link,
	}
	if err := appendData(sheetsSrv, cfg.SheetID, data); err != nil {
		sendError(bot, msg.Chat.ID, cfg.AdminID, fmt.Sprintf("Sheet update failed: %v", err), msg)
		return
	}
	sendSuccess(bot, msg.Chat.ID, cfg.AdminID, data, msg, folderMsg)
}

func processMediaGroup(bot *tgbotapi.BotAPI, groupID string, driveSrv *drive.Service, sheetsSrv *sheets.Service, cfg Config) {
	time.Sleep(500 * time.Millisecond)
	mediaCacheMu.Lock()
	group, ok := mediaCache[groupID]
	if !ok || group.Processing {
		mediaCacheMu.Unlock()
		return
	}
	group.Processing = true
	photos := make([]*tgbotapi.PhotoSize, 0, len(group.Photos))
	for _, p := range group.Photos {
		photos = append(photos, p)
	}
	mediaCacheMu.Unlock()

	folderID, folderMsg, err := ensureFolder(driveSrv, cfg.DriveFolderID, group.Address)
	if err != nil {
		sendError(bot, group.ChatID, cfg.AdminID, fmt.Sprintf("Folder error: %v", err), nil)
		return
	}
	links := uploadPhotos(bot, driveSrv, photos, folderID, group.Address, group.Amount)
	if len(links) == 0 {
		sendError(bot, group.ChatID, cfg.AdminID, "Failed to upload photos", nil)
		return
	}
	data := ParsedData{
		Address:   group.Address,
		Amount:    group.Amount,
		Comment:   group.Comment,
		Username:  group.Username,
		Date:      time.Now().Format("02.01.2006 15:04:05"),
		DriveLink: strings.Join(links, " "),
	}
	if err := appendData(sheetsSrv, cfg.SheetID, data); err != nil {
		sendError(bot, group.ChatID, cfg.AdminID, fmt.Sprintf("Sheet update failed: %v", err), nil)
		return
	}
	mediaCacheMu.Lock()
	delete(mediaCache, groupID)
	mediaCacheMu.Unlock()
	sendSuccess(bot, group.ChatID, cfg.AdminID, data, nil, folderMsg)
}

func uploadPhotos(bot *tgbotapi.BotAPI, driveSrv *drive.Service, photos []*tgbotapi.PhotoSize, folderID, addr, amt string) []string {
	results := make(chan string, len(photos))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrent)

	for i, photo := range photos {
		wg.Add(1)
		go func(idx int, p *tgbotapi.PhotoSize) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if link, err := uploadPhotoToDrive(bot, driveSrv, p.FileID, folderID, addr, amt, idx+1); err == nil {
				results <- link
			}
		}(i, photo)
	}
	wg.Wait()
	close(results)

	var links []string
	for link := range results {
		links = append(links, link)
	}
	return links
}

func handleMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, driveSrv *drive.Service, sheetsSrv *sheets.Service, cfg Config) {
	switch {
	case msg.IsCommand() && (msg.Command() == "start" || msg.Command() == "help"):
		sendHelp(bot, msg.Chat.ID)
	case msg.Text == "Начать" || msg.Text == "Вернуться":
		sendHelp(bot, msg.Chat.ID)
	case msg.Text == "Объекты":
		sendObjects(bot, msg.Chat.ID)
	case len(msg.Photo) > 0:
		if msg.MediaGroupID == "" {
			go processSinglePhoto(bot, msg, driveSrv, sheetsSrv, cfg)
		} else {
			go handleMediaGroupMessage(bot, msg, driveSrv, sheetsSrv, cfg)
		}
	}
}

func handleMediaGroupMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, driveSrv *drive.Service, sheetsSrv *sheets.Service, cfg Config) {
	mediaCacheMu.Lock()
	defer mediaCacheMu.Unlock()

	group, exists := mediaCache[msg.MediaGroupID]
	if !exists {
		addr, amt, comm, err := parseCaption(msg.Caption)
		if err != nil {
			bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Invalid caption. Use: Address: ... Sum: ..."))
			return
		}
		group = &MediaGroup{
			Photos:    make(map[string]*tgbotapi.PhotoSize),
			Address:   addr,
			Amount:    amt,
			Comment:   comm,
			Username:  getUserName(msg.From),
			ChatID:    msg.Chat.ID,
			UpdatedAt: time.Now(),
		}
		mediaCache[msg.MediaGroupID] = group
	}
	best := msg.Photo[len(msg.Photo)-1]
	group.Photos[best.FileID] = &best
	group.UpdatedAt = time.Now()

	if len(group.Photos) >= 2 || time.Since(group.UpdatedAt) >= time.Second {
		go processMediaGroup(bot, msg.MediaGroupID, driveSrv, sheetsSrv, cfg)
	}
}

func sendHelp(bot *tgbotapi.BotAPI, chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "👋 Welcome! Send a photo with caption:\nAddress: ...\nSum: ...\nComment: ... (optional)")
	msg.ReplyMarkup = tgbotapi.NewReplyKeyboard(
		[]tgbotapi.KeyboardButton{tgbotapi.NewKeyboardButton("Начать")},
		[]tgbotapi.KeyboardButton{tgbotapi.NewKeyboardButton("Объекты")},
	)
	bot.Send(msg)
}

func sendObjects(bot *tgbotapi.BotAPI, chatID int64) {
	text := "📍 Objects:\n" + strings.Join(objects, "\n")
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ReplyMarkup = tgbotapi.NewReplyKeyboard([]tgbotapi.KeyboardButton{tgbotapi.NewKeyboardButton("Вернуться")})
	bot.Send(msg)
}

func sendSuccess(bot *tgbotapi.BotAPI, chatID, adminID int64, data ParsedData, msg *tgbotapi.Message, folderMsg string) {
	userMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Uploaded!\nAddress: %s\nSum: %s", data.Address, data.Amount))
	bot.Send(userMsg)
	adminMsg := tgbotapi.NewMessage(adminID, fmt.Sprintf("✅ Success\nUser: %s\nAddress: %s\nSum: %s\nLink: %s\nFolder: %s", data.Username, data.Address, data.Amount, data.DriveLink, folderMsg))
	bot.Send(adminMsg)
}

func sendError(bot *tgbotapi.BotAPI, chatID, adminID int64, errMsg string, msg *tgbotapi.Message) {
	userMsg := tgbotapi.NewMessage(chatID, "❗️ Error: "+errMsg)
	bot.Send(userMsg)
	adminMsg := tgbotapi.NewMessage(adminID, fmt.Sprintf("❗️ Error\nUser: %s\nMessage: %s\nError: %s", getUserName(msg.From), msg.Text, errMsg))
	bot.Send(adminMsg)
}

func getUserName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return user.FirstName + " " + user.LastName
	}
	return user.FirstName
}

// Главная функция
func main() {
	cfg := loadConfig()
	oauthCfg := initOAuth(cfg)
	client := getClient(oauthCfg)

	sheetsSrv, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Sheets service failed: %v", err)
	}
	driveSrv, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Drive service failed: %v", err)
	}
	bot, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		log.Fatalf("Bot init failed: %v", err)
	}

	webhookURL, _ := url.Parse(cfg.WebhookURL)
	bot.Request(tgbotapi.WebhookConfig{URL: webhookURL, MaxConnections: 40})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var update tgbotapi.Update
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		if update.Message != nil {
			handleMessage(bot, update.Message, driveSrv, sheetsSrv, cfg)
		}
		w.WriteHeader(http.StatusOK)
	})

	go keepAlive(cfg.WebhookURL)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: ":" + port}
	go srv.ListenAndServe()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Shutdown(context.Background())
}

func keepAlive(url string) {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		http.Get(url)
	}
}
