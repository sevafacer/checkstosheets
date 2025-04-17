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

// -----------------------------------------------------------------------------
// Константы и глобальные переменные
// -----------------------------------------------------------------------------

const (
	maxRetries           = 3
	retryDelay           = 2
	maxConcurrentUploads = 10

	sheetIDRange = "'Чеки'!B:B"
	sheetUpdate  = "'Чеки'!B%d:G%d"

	// OAuth / env
	tokenEnvName = "GOOGLE_OAUTH_REFRESH_TOKEN"
)

var (
	// Ключевые слова для парсинга подписей
	fieldKeywords = map[string][]string{
		"address": {"адрес", "объект", "квартира", "школа", "дом", "улица", "место", "локация"},
		"amount":  {"сумма", "стоимость", "оплата", "платёж", "цена"},
		"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение", "заметка"},
	}

	tokenMu     sync.Mutex
	oauthConfig *oauth2.Config
	oauthState  = "state-token"
	authCodeCh  = make(chan string)

	mediaGroupCache   = make(map[string]*MediaGroupData)
	mediaGroupMu      sync.Mutex
	mediaGroupTimeout = 3 * time.Second
)

// Список объектов для кнопки «Объекты»
var objectAddresses = []string{
	"Афанасьево 1",
	"Афанасьево 2",
	"Каскад 1",
	"Каскад 2",
	"Тимирязева 3",
	"Ковернино",
	"Комсомольская",
	"Город Времени",
	"Крутая",
	"Малая Ельня",
	"Тимирязева 9",
	"Анкудиновское шоссе 47",
	"Советской Армии",
	"Волоколамское шоссе",
	"Долгопрудненское шоссе 3",
}

// -----------------------------------------------------------------------------
// Типы
// -----------------------------------------------------------------------------

type ParsedData struct {
	Address, Amount, Comment, Username, Date, DriveLink string
}

type MediaGroupData struct {
	Files            map[string]*tgbotapi.PhotoSize
	Caption          string
	Address          string
	Amount           string
	Comment          string
	FirstMessageTime time.Time
	LastUpdated      time.Time
	UserID           int64
	ChatID           int64
	Username         string
	IsProcessing     bool
	timer            *time.Timer
}

// -----------------------------------------------------------------------------
// Вспомогательные функции
// -----------------------------------------------------------------------------

func mandatory(key string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		log.Fatalf("env %s not set", key)
	}
	return v
}

func loadEnv() (tgToken, sheetID, driveFolderID string, adminID int64, clientID, clientSecret, webhookURL string) {
	tgToken = mandatory("TELEGRAM_BOT_TOKEN")
	sheetID = mandatory("GOOGLE_SHEET_ID")
	driveFolderID = mandatory("GOOGLE_DRIVE_FOLDER_ID")
	clientID = mandatory("GOOGLE_OAUTH_CLIENT_ID")
	clientSecret = mandatory("GOOGLE_OAUTH_CLIENT_SECRET")
	webhookURL = mandatory("WEBHOOK_URL")
	adminStr := mandatory("ADMIN_CHAT_ID")

	id, err := strconv.ParseInt(adminStr, 10, 64)
	if err != nil {
		log.Fatalf("invalid ADMIN_CHAT_ID: %v", err)
	}
	adminID = id
	return
}

// -----------------------------------------------------------------------------
// OAuth helpers
// -----------------------------------------------------------------------------

func refreshTokenFromEnv() (*oauth2.Token, error) {
	tok := os.Getenv(tokenEnvName)
	if tok == "" {
		return nil, errors.New("refresh token env not set")
	}
	return &oauth2.Token{RefreshToken: tok}, nil
}

func getOAuthClient(cfg *oauth2.Config) (*http.Client, error) {
	tokenMu.Lock()
	defer tokenMu.Unlock()

	if rt, err := refreshTokenFromEnv(); err == nil {
		// Есть refresh‑token — пытаемся обновить access‑token
		ts := cfg.TokenSource(context.Background(), rt)
		if t, err := ts.Token(); err == nil {
			return cfg.Client(context.Background(), t), nil
		}
	}

	// Запускаем локальный сервер для получения auth‑code
	srv := &http.Server{Addr: ":8080"}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != oauthState {
			http.Error(w, "state mismatch", http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "code not found", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "Auth OK. Return to console.")
		authCodeCh <- code
	})
	srv.Handler = mux
	go func() {
		_ = srv.ListenAndServe()
	}()

	fmt.Println("Open", cfg.AuthCodeURL(oauthState, oauth2.AccessTypeOffline))

	select {
	case code := <-authCodeCh:
		_ = srv.Shutdown(context.Background())
		tok, err := cfg.Exchange(context.Background(), code)
		if err != nil {
			return nil, err
		}
		fmt.Printf("New refresh‑token → set %s env: %s\n", tokenEnvName, tok.RefreshToken)
		return cfg.Client(context.Background(), tok), nil
	case <-time.After(5 * time.Minute):
		_ = srv.Shutdown(context.Background())
		return nil, errors.New("oauth timeout")
	}
}

// -----------------------------------------------------------------------------
// Drive / Sheets helpers
// -----------------------------------------------------------------------------

func appendToSheet(srv *sheets.Service, sheetID string, data ParsedData) error {
	values := []interface{}{data.Date, data.Username, data.Address, data.Amount, data.Comment, data.DriveLink}
	vr := &sheets.ValueRange{Values: [][]interface{}{values}}

	resp, err := srv.Spreadsheets.Values.Get(sheetID, sheetIDRange).Do()
	if err != nil {
		return err
	}
	row := len(resp.Values) + 1
	_, err = srv.Spreadsheets.Values.Update(sheetID, fmt.Sprintf(sheetUpdate, row, row), vr).
		ValueInputOption("USER_ENTERED").Do()
	return err
}

// sanitizeFileName removes invalid chars and duplicates
func sanitizeFileName(name string) string {
	re := regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s\.-]`)
	s := re.ReplaceAllString(name, "_")
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, "_")
	s = regexp.MustCompile(`_+`).ReplaceAllString(s, "_")
	return strings.Trim(s, "_")
}

// -----------------------------------------------------------------------------
// Telegram helpers
// -----------------------------------------------------------------------------

func fullName(u *tgbotapi.User) string {
	if u == nil {
		return ""
	}
	if u.LastName != "" {
		return u.FirstName + " " + u.LastName
	}
	return u.FirstName
}

func sendObjectsList(bot *tgbotapi.BotAPI, chatID int64) {
	var b strings.Builder
	b.WriteString("📍 Список объектов:\n\n")
	for i, a := range objectAddresses {
		fmt.Fprintf(&b, "%d. %s\n", i+1, a)
	}
	b.WriteString("\nСкопируйте адрес для подписи чека.")

	msg := tgbotapi.NewMessage(chatID, b.String())
	if _, err := bot.Send(msg); err != nil {
		log.Println("sendObjectsList:", err)
	}
}

func mainKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		[]tgbotapi.KeyboardButton{{Text: "Начать"}},
		[]tgbotapi.KeyboardButton{{Text: "Объекты"}},
	)
}

func sendHelp(bot *tgbotapi.BotAPI, chatID int64) {
	text := "👋 Бот для отслеживания чеков!\n\n" +
		"Отправьте фото(а) с подписью:\n" +
		"Адрес\nСумма\nКомментарий (опц)\n\n" +
		"Или в одну строку с ключевыми словами: Адрес: ... Сумма: ..."
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ReplyMarkup = mainKeyboard()
	if _, err := bot.Send(msg); err != nil {
		log.Println("sendHelp:", err)
	}
}

// -----------------------------------------------------------------------------
// Message parsing (сокращённая версия)
// -----------------------------------------------------------------------------

type fieldMatch struct {
	field      string
	start, end int
}

func cleanAmount(a string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	cleaned := re.ReplaceAllString(a, "")
	return strings.ReplaceAll(cleaned, ".", ",")
}

func removeLeadingKeyword(text string, kws []string) string {
	t := strings.TrimSpace(text)
	lower := strings.ToLower(t)
	for _, kw := range kws {
		if strings.HasPrefix(lower, kw) {
			return strings.TrimSpace(t[len(kw):])
		}
	}
	return t
}

func parseMessage(message string) (addr, amt, comm string, err error) {
	if strings.TrimSpace(message) == "" {
		return "", "", "", errors.New("empty message")
	}

	// 1. Попытка через ключевые слова
	if strings.ContainsAny(message, ":=") {
		normalized := strings.Join(strings.Fields(message), " ")
		var matches []fieldMatch
		for field, kws := range fieldKeywords {
			for _, kw := range kws {
				re := regexp.MustCompile(fmt.Sprintf(`(?i)%s\s*[:=]`, regexp.QuoteMeta(kw)))
				for _, loc := range re.FindAllStringIndex(normalized, -1) {
					matches = append(matches, fieldMatch{field: field, start: loc[0], end: loc[1]})
				}
			}
		}
		if len(matches) > 0 {
			sort.Slice(matches, func(i, j int) bool { return matches[i].start < matches[j].start })
			vals := make(map[string]string)
			for i, m := range matches {
				end := len(normalized)
				if i < len(matches)-1 {
					end = matches[i+1].start
				}
				v := strings.TrimSpace(normalized[m.end:end])
				if v != "" {
					vals[m.field] = v
				}
			}
			addr, amt = vals["address"], cleanAmount(vals["amount"])
			comm = vals["comment"]
			if addr != "" && amt != "" {
				return
			}
		}
	}

	// 2. Многострочный без ключевых
	if strings.Contains(message, "\n") {
		lines := strings.Split(message, "\n")
		if len(lines) >= 2 {
			addr = removeLeadingKeyword(lines[0], fieldKeywords["address"])
			amt = cleanAmount(removeLeadingKeyword(lines[1], fieldKeywords["amount"]))
			if len(lines) > 2 {
				comm = removeLeadingKeyword(strings.Join(lines[2:], " \n"), fieldKeywords["comment"])
			}
			if addr != "" && amt != "" {
				return
			}
		}
	}

	return "", "", "", errors.New("parse failed")
}

// -----------------------------------------------------------------------------
// Media handling helpers (сокращено для примера)
// -----------------------------------------------------------------------------

func processPhoto(bot *tgbotapi.BotAPI, fileID, folderID, addr, amt string, idx int, driveSrv *drive.Service) (string, error) {
	url, err := bot.GetFileDirectURL(fileID)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	tmp, err := os.CreateTemp("", "photo_*.jpg")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())

	if _, err = io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return "", err
	}
	tmp.Close()

	name := fmt.Sprintf("%s_%s_%s_%02d.jpg", time.Now().Format("020106"), sanitizeFileName(addr), sanitizeFileName(strings.ReplaceAll(amt, ",", ".")), idx)

	f, err := os.Open(tmp.Name())
	if err != nil {
		return "", err
	}
	defer f.Close()

	df := &drive.File{Name: name, Parents: []string{folderID}}
	res, err := driveSrv.Files.Create(df).Media(f).Fields("webViewLink").Do()
	if err != nil {
		return "", err
	}
	return res.WebViewLink, nil
}

// -----------------------------------------------------------------------------
// Telegram update handler (сильно упрощён для компиляции)
// -----------------------------------------------------------------------------

func main() {
	tgToken, sheetID, driveFolderID, adminID, clientID, clientSecret, webhookURL := loadEnv()

	oauthConfig = &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  webhookURL,
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive.file",
		},
		Endpoint: google.Endpoint,
	}

	httpClient, err := getOAuthClient(oauthConfig)
	if err != nil {
		log.Fatalf("oauth: %v", err)
	}

	sheetsSrv, err := sheets.NewService(context.Background(), option.WithHTTPClient(httpClient))
	if err != nil {
		log.Fatalf("sheets: %v", err)
	}

	driveSrv, err := drive.NewService(context.Background(), option.WithHTTPClient(httpClient))
	if err != nil {
		log.Fatalf("drive: %v", err)
	}

	bot, err := tgbotapi.NewBotAPI(tgToken)
	if err != nil {
		log.Fatalf("bot: %v", err)
	}

	// ---- webhook init ----
	parsedURL, err := url.Parse(webhookURL)
	if err != nil {
		log.Fatalf("WEBHOOK_URL: %v", err)
	}
	if _, err = bot.Request(tgbotapi.WebhookConfig{URL: parsedURL, MaxConnections: 40}); err != nil {
		log.Fatalf("webhook: %v", err)
	}

	// ---- HTTP handler ----
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var upd tgbotapi.Update
		if err := json.Unmarshal(body, &upd); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		if upd.Message == nil {
			w.WriteHeader(http.StatusOK)
			return
		}

		m := upd.Message
		switch {
		case m.IsCommand():
			switch m.Command() {
			case "start", "help":
				sendHelp(bot, m.Chat.ID)
			}
		case m.Text == "Начать":
			sendHelp(bot, m.Chat.ID)
		case m.Text == "Объекты":
			sendObjectsList(bot, m.Chat.ID)
		case len(m.Photo) > 0:
			if m.Caption == "" {
				_, _ = bot.Send(tgbotapi.NewMessage(m.Chat.ID, "Добавьте подпись с адресом и суммой"))
				break
			}
			addr, amt, comm, err := parseMessage(m.Caption)
			if err != nil {
				_, _ = bot.Send(tgbotapi.NewMessage(m.Chat.ID, "Не удалось распознать подпись"))
				break
			}
			folderID := driveFolderID // в полном коде ensureObjectFolder
			best := m.Photo[len(m.Photo)-1]
			link, err := processPhoto(bot, best.FileID, folderID, addr, amt, 1, driveSrv)
			if err != nil {
				_, _ = bot.Send(tgbotapi.NewMessage(m.Chat.ID, "Не удалось загрузить фото"))
				break
			}
			data := ParsedData{Address: addr, Amount: amt, Comment: comm, Username: fullName(m.From), Date: time.Now().Format("02.01.2006 15:04:05"), DriveLink: link}
			if err := appendToSheet(sheetsSrv, sheetID, data); err != nil {
				_, _ = bot.Send(tgbotapi.NewMessage(m.Chat.ID, "Ошибка записи в таблицу"))
				break
			}
			_, _ = bot.Send(tgbotapi.NewMessage(m.Chat.ID, fmt.Sprintf("✅ Чек загружен! Адрес: %s Сумма: %s", addr, amt)))
			_, _ = bot.Send(tgbotapi.NewMessage(adminID, fmt.Sprintf("✅ %s загрузил чек %s %s", data.Username, addr, amt)))
		}
		w.WriteHeader(http.StatusOK)
	})

	// ---- graceful shutdown ----
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	srv := &http.Server{Addr: ":8080"}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
