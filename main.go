package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

type Config struct {
	TelegramToken        string
	SpreadsheetID        string
	DriveFolderID        string
	AdminID              int64
	GoogleClientID       string
	GoogleClientSecret   string
	WebhookURL           string
	Port                 string
	AppURL               string // Base URL for OAuth callback, e.g., https://myapp.up.railway.app
	TokenStorageMethod   string // "file" or "env"
	TokenFilePath        string // Path for token file if method is "file"
	TokenEnvVar          string // Env var name if method is "env"
	MaxConcurrentUploads int
	MediaGroupTimeout    time.Duration
	TokenRefreshWindow   time.Duration
	MaxRetries           int
	RetryDelay           time.Duration
}

const (
	defaultPort               = "8080"
	defaultTokenFile          = "token.json"
	defaultTokenEnvVar        = "GOOGLE_OAUTH_TOKEN_JSON_BASE64"
	defaultTokenStorageMethod = "file" // Or "env"
	defaultMaxRetries         = 3
	defaultRetryDelay         = 2 * time.Second
	defaultTokenRefreshWindow = 5 * time.Minute
	defaultMaxConcurrent      = 5
	defaultMediaGroupTimeout  = 3 * time.Second
	oauthCallbackPath         = "/oauth/callback"
	authPath                  = "/auth"
	webhookPath               = "/webhook" // Use a unique path
)

var (
	fieldKeywords = map[string][]string{
		"address": {"адрес", "объект", "квартира", "школа", "дом", "улица", "место", "локация"},
		"amount":  {"сумма", "стоимость", "оплата", "платёж", "цена"},
		"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение", "заметка"},
	}
	objectAddresses = []string{
		"Афанасьево 1", "Афанасьево 2", "Каскад 1", "Каскад 2", "Тимирязева 3",
		"Ковернино", "Комсомольская", "Город Времени", "Крутая", "Малая Ельня",
		"Тимирязева 9", "Анкудиновское шоссе 47", "Советской Армии",
		"Волоколамское шоссе", "Долгопрудненское шоссе 3",
	}
	amountCleanRegex         = regexp.MustCompile(`[^0-9.,]`)
	fileNameSanitizeRegex    = regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s.-]`)
	multipleSpacesRegex      = regexp.MustCompile(`\s+`)
	multipleUnderscoresRegex = regexp.MustCompile(`_+`)
	oauthState               = "state-token-replace-with-random" // Should be random per request
)

func LoadConfig() (*Config, error) {
	cfg := &Config{
		TelegramToken:        os.Getenv("TELEGRAM_BOT_TOKEN"),
		SpreadsheetID:        os.Getenv("GOOGLE_SHEET_ID"),
		DriveFolderID:        os.Getenv("GOOGLE_DRIVE_FOLDER_ID"),
		GoogleClientID:       os.Getenv("GOOGLE_OAUTH_CLIENT_ID"),
		GoogleClientSecret:   os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET"),
		WebhookURL:           os.Getenv("WEBHOOK_URL"), // Full webhook URL for Telegram setup
		AppURL:               os.Getenv("APP_URL"),     // Base URL of the app itself
		Port:                 os.Getenv("PORT"),
		TokenStorageMethod:   os.Getenv("TOKEN_STORAGE_METHOD"),
		TokenFilePath:        os.Getenv("TOKEN_FILE_PATH"),
		TokenEnvVar:          os.Getenv("TOKEN_ENV_VAR"),
		MaxConcurrentUploads: defaultMaxConcurrent,
		MediaGroupTimeout:    defaultMediaGroupTimeout,
		TokenRefreshWindow:   defaultTokenRefreshWindow,
		MaxRetries:           defaultMaxRetries,
		RetryDelay:           defaultRetryDelay,
	}

	adminStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	var err error
	if adminStr != "" {
		cfg.AdminID, err = strconv.ParseInt(adminStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid ADMIN_CHAT_ID format: %w", err)
		}
	}

	if cfg.TelegramToken == "" || cfg.SpreadsheetID == "" || cfg.DriveFolderID == "" ||
		cfg.GoogleClientID == "" || cfg.GoogleClientSecret == "" || cfg.WebhookURL == "" || cfg.AppURL == "" {
		return nil, errors.New("missing one or more required environment variables (TELEGRAM_BOT_TOKEN, GOOGLE_SHEET_ID, GOOGLE_DRIVE_FOLDER_ID, GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, WEBHOOK_URL, APP_URL)")
	}

	if cfg.Port == "" {
		cfg.Port = defaultPort
	}
	if cfg.TokenStorageMethod == "" {
		cfg.TokenStorageMethod = defaultTokenStorageMethod
	}
	if cfg.TokenStorageMethod == "file" && cfg.TokenFilePath == "" {
		cfg.TokenFilePath = defaultTokenFile
	}
	if cfg.TokenStorageMethod == "env" && cfg.TokenEnvVar == "" {
		cfg.TokenEnvVar = defaultTokenEnvVar
	}
	if cfg.TokenStorageMethod != "file" && cfg.TokenStorageMethod != "env" {
		return nil, fmt.Errorf("invalid TOKEN_STORAGE_METHOD: must be 'file' or 'env'")
	}

	return cfg, nil
}

// --- Models ---

type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string // Space-separated for multiple files
	ChatID    int64
	UserID    int64
	SourceMsg *tgbotapi.Message // Keep original message for context if needed
}

type MediaGroupCacheEntry struct {
	Files        map[string]*tgbotapi.PhotoSize // fileID -> photoSize
	Caption      string
	Address      string
	Amount       string
	Comment      string
	UserID       int64
	ChatID       int64
	Username     string
	IsProcessing bool
	ProcessTimer *time.Timer
	LastUpdated  time.Time
	SourceMsg    *tgbotapi.Message // First message for context
}

// --- Feedback Handler ---

type FeedbackHandler struct {
	bot     *tgbotapi.BotAPI
	adminID int64
}

func NewFeedbackHandler(bot *tgbotapi.BotAPI, adminID int64) *FeedbackHandler {
	return &FeedbackHandler{bot: bot, adminID: adminID}
}

func (h *FeedbackHandler) send(msg tgbotapi.MessageConfig) {
	if _, err := h.bot.Send(msg); err != nil {
		slog.Error("Failed to send message", "error", err, "chat_id", msg.ChatID)
	}
}

func (h *FeedbackHandler) SendSuccess(chatID int64, data *ParsedData, fileCount, totalFiles int) {
	text := fmt.Sprintf("✅ Чек успешно загружен!\nАдрес: %s\nСумма: %s", data.Address, data.Amount)
	if data.Comment != "" {
		text += fmt.Sprintf("\nКомментарий: %s", data.Comment)
	}
	if totalFiles > 1 {
		text += fmt.Sprintf("\nФото: %d/%d обработано", fileCount, totalFiles)
	}
	msg := tgbotapi.NewMessage(chatID, text)
	h.send(msg)
}

func (h *FeedbackHandler) SendParsingError(chatID int64, userErr string) {
	text := fmt.Sprintf("❗️ Ошибка распознавания данных.\n%s\n\nПожалуйста, проверьте формат подписи. Используйте:\nАдрес: ...\nСумма: ...\nКомментарий: ... (необязательно)", userErr)
	msg := tgbotapi.NewMessage(chatID, text)
	h.send(msg)
}

func (h *FeedbackHandler) SendError(chatID int64, userMsg string) {
	text := fmt.Sprintf("❗️ Произошла ошибка: %s", userMsg)
	msg := tgbotapi.NewMessage(chatID, text)
	h.send(msg)
}

func (h *FeedbackHandler) SendSimpleReply(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	h.send(msg)
}

func (h *FeedbackHandler) NotifyAdminSuccess(data *ParsedData, folderMsg string, fileCount, totalFiles int) {
	if h.adminID == 0 {
		return
	}
	var origMsgText string
	if data.SourceMsg != nil {
		origMsgText = data.SourceMsg.Text
		if data.SourceMsg.Caption != "" {
			origMsgText = data.SourceMsg.Caption
		}
	}

	text := fmt.Sprintf("✅ Успех [%d]\nПользователь: %s (%d)\nВремя: %s\nАдрес: %s\nСумма: %s\nКоммент: %s\nФайлы (%d/%d): %s\nПапка: %s\nИсходное: %s",
		data.ChatID, data.Username, data.UserID, data.Date, data.Address, data.Amount, data.Comment, fileCount, totalFiles, data.DriveLink, folderMsg, origMsgText)
	msg := tgbotapi.NewMessage(h.adminID, text)
	h.send(msg)
}

func (h *FeedbackHandler) NotifyAdminFailure(chatID, userID int64, username string, sourceMsg *tgbotapi.Message, operation string, err error) {
	if h.adminID == 0 {
		return
	}
	var origMsgText string
	if sourceMsg != nil {
		origMsgText = sourceMsg.Text
		if sourceMsg.Caption != "" {
			origMsgText = sourceMsg.Caption
		}
	}
	text := fmt.Sprintf("❗️ Ошибка [%d]\nОперация: %s\nПользователь: %s (%d)\nОшибка: %v\nИсходное: %s",
		chatID, operation, username, userID, err, origMsgText)
	msg := tgbotapi.NewMessage(h.adminID, text)
	h.send(msg)
}

func (h *FeedbackHandler) NotifyAdminCritical(operation string, err error) {
	slog.Error("Critical Failure", "operation", operation, "error", err)
	if h.adminID == 0 {
		return
	}
	text := fmt.Sprintf("🆘 КРИТИЧЕСКАЯ ОШИБКА\nОперация: %s\nОшибка: %v\n\nТребуется вмешательство!", operation, err)
	msg := tgbotapi.NewMessage(h.adminID, text)
	h.send(msg)
}

// --- Parser ---

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

type fieldMatch struct {
	field      string
	start, end int
}

func (p *Parser) ParseMessage(message string) (addr, amount, comment string, err error) {
	message = strings.TrimSpace(message)
	if message == "" {
		err = errors.New("пустая подпись")
		return
	}

	normalized := strings.Join(strings.Fields(message), " ")
	var matches []fieldMatch

	for field, kws := range fieldKeywords {
		for _, kw := range kws {
			pattern := fmt.Sprintf("(?i)%s\\s*[:=]", regexp.QuoteMeta(kw))
			re := regexp.MustCompile(pattern)
			foundIndices := re.FindAllStringIndex(normalized, -1)
			for _, loc := range foundIndices {
				matches = append(matches, fieldMatch{field: field, start: loc[0], end: loc[1]})
			}
		}
	}

	if len(matches) > 0 {
		sort.Slice(matches, func(i, j int) bool { return matches[i].start < matches[j].start })
		fieldValues := make(map[string]string)
		lastPos := 0
		if matches[0].start > 0 { // Text before first keyword might be address or comment
			preText := strings.TrimSpace(normalized[:matches[0].start])
			if preText != "" {
				// Heuristic: if it looks like an address, assign it, otherwise comment
				if len(strings.Fields(preText)) > 1 { // Simple check
					fieldValues["address"] = preText
				} else {
					fieldValues["comment"] = preText
				}
			}
		}

		for i, m := range matches {
			endPos := len(normalized)
			if i < len(matches)-1 {
				endPos = matches[i+1].start
			}
			val := strings.TrimSpace(normalized[m.end:endPos])

			if _, exists := fieldValues[m.field]; !exists && val != "" {
				fieldValues[m.field] = val
			} else if val != "" && m.field == "comment" { // Allow appending to comment
				fieldValues["comment"] += " " + val
			}
			lastPos = endPos
		}

		// Handle text after the last keyword as comment if not already assigned
		if lastPos < len(normalized) {
			postText := strings.TrimSpace(normalized[lastPos:])
			if postText != "" {
				if _, exists := fieldValues["comment"]; exists {
					fieldValues["comment"] += " " + postText
				} else {
					fieldValues["comment"] = postText
				}
			}
		}

		addr = strings.TrimSpace(fieldValues["address"])
		amount = cleanAmount(fieldValues["amount"])
		comment = strings.TrimSpace(fieldValues["comment"])

		if addr == "" || amount == "" {
			addr, amount, comment, err = p.fallbackParse(message)
			if err != nil {
				err = fmt.Errorf("не удалось извлечь адрес или сумму из подписи")
			}
			return
		}
		return addr, amount, comment, nil
	}

	return p.fallbackParse(message)
}

func (p *Parser) fallbackParse(message string) (addr, amount, comment string, err error) {
	lines := strings.Split(message, "\n")
	cleanedLines := []string{}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			cleanedLines = append(cleanedLines, trimmed)
		}
	}

	if len(cleanedLines) >= 2 {
		addr = removeLeadingKeyword(cleanedLines[0], fieldKeywords["address"])
		amount = cleanAmount(removeLeadingKeyword(cleanedLines[1], fieldKeywords["amount"]))
		if len(cleanedLines) > 2 {
			comment = removeLeadingKeyword(strings.Join(cleanedLines[2:], "\n"), fieldKeywords["comment"])
		}
		if addr == "" || amount == "" {
			err = errors.New("не найден адрес или сумма (формат по строкам)")
			return
		}
		return addr, amount, comment, nil
	}

	// Single line fallback (less reliable)
	if len(cleanedLines) == 1 {
		// Try to find amount keyword
		lowerMsg := strings.ToLower(cleanedLines[0])
		amountIdx := -1
		// amountKwLen := 0 // Not needed
		for _, kw := range fieldKeywords["amount"] {
			if idx := strings.Index(lowerMsg, kw); idx != -1 {
				// Basic check if it's followed by numbers
				potentialAmount := strings.TrimSpace(cleanedLines[0][idx+len(kw):])
				if len(potentialAmount) > 0 && (potentialAmount[0] >= '0' && potentialAmount[0] <= '9') {
					amountIdx = idx
					// amountKwLen = len(kw) // Not needed
					break
				}
			}
		}

		if amountIdx != -1 {
			addr = removeLeadingKeyword(strings.TrimSpace(cleanedLines[0][:amountIdx]), fieldKeywords["address"])
			amount = cleanAmount(removeLeadingKeyword(strings.TrimSpace(cleanedLines[0][amountIdx:]), fieldKeywords["amount"]))

			// Assume anything after amount is comment (simplistic)
			// This part is tricky without more context or keywords
			// Let's just take the amount for now.

			if addr == "" || amount == "" {
				err = errors.New("не найден адрес или сумма (формат одной строкой)")
				return
			}
			return addr, amount, "", nil // Comment parsing is too ambiguous here
		}
	}

	err = errors.New("не удалось распознать формат подписи")
	return
}

func cleanAmount(amount string) string {
	cleaned := amountCleanRegex.ReplaceAllString(amount, "")
	return strings.ReplaceAll(cleaned, ".", ",")
}

func removeLeadingKeyword(text string, keywords []string) string {
	trimmed := strings.TrimSpace(text)
	lower := strings.ToLower(trimmed)
	for _, kw := range keywords {
		prefix := kw + ":"
		prefixEq := kw + "="
		if strings.HasPrefix(lower, prefix) {
			return strings.TrimSpace(trimmed[len(prefix):])
		}
		if strings.HasPrefix(lower, prefixEq) {
			return strings.TrimSpace(trimmed[len(prefixEq):])
		}
		// Check for keyword without separator if it's likely followed by value
		if strings.HasPrefix(lower, kw+" ") {
			potentialValue := trimmed[len(kw):]
			if len(potentialValue) > 0 && potentialValue[0] == ' ' { // Ensure space separation
				return strings.TrimSpace(potentialValue)
			}
		}
	}
	return trimmed
}

func sanitizeFileName(name string) string {
	s := fileNameSanitizeRegex.ReplaceAllString(name, "_")
	s = multipleSpacesRegex.ReplaceAllString(s, "_")
	s = multipleUnderscoresRegex.ReplaceAllString(s, "_")
	return strings.Trim(s, "_")
}

// --- Google API Client ---

// --- OAuth Handler ---

type OAuthHandler struct {
	config         *oauth2.Config
	token          *oauth2.Token
	tokenMutex     sync.Mutex
	storageMethod  string
	tokenFilePath  string
	tokenEnvVar    string
	feedback       *FeedbackHandler
	initialAuthLog sync.Once
}

func NewOAuthHandler(cfg *Config, feedback *FeedbackHandler) (*OAuthHandler, error) {
	handler := &OAuthHandler{
		config: &oauth2.Config{
			ClientID:     cfg.GoogleClientID,
			ClientSecret: cfg.GoogleClientSecret,
			RedirectURL:  cfg.AppURL + oauthCallbackPath,
			Scopes: []string{
				"https://www.googleapis.com/auth/spreadsheets",
				"https://www.googleapis.com/auth/drive.file",
			},
			Endpoint: google.Endpoint,
		},
		storageMethod: cfg.TokenStorageMethod,
		tokenFilePath: cfg.TokenFilePath,
		tokenEnvVar:   cfg.TokenEnvVar,
		feedback:      feedback,
	}

	err := handler.loadToken()
	if err != nil {
		slog.Warn("Failed to load initial token, manual authorization required", "error", err, "storage_method", handler.storageMethod)
		// Don't return error, allow GetClient to trigger auth URL message
	} else {
		slog.Info("OAuth token loaded successfully", "storage_method", handler.storageMethod)
	}

	return handler, nil
}

func (h *OAuthHandler) saveToken(token *oauth2.Token) error {
	h.tokenMutex.Lock()
	defer h.tokenMutex.Unlock()

	data, err := json.MarshalIndent(token, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal token: %w", err)
	}

	if h.storageMethod == "file" {
		if err := os.WriteFile(h.tokenFilePath, data, 0600); err != nil {
			return fmt.Errorf("failed to write token file '%s': %w", h.tokenFilePath, err)
		}
		slog.Info("OAuth token saved to file", "path", h.tokenFilePath)
	} else if h.storageMethod == "env" {
		encoded := base64.StdEncoding.EncodeToString(data)
		slog.Warn("=== ACTION REQUIRED ===")
		slog.Warn("OAuth token received/refreshed. To persist it using 'env' method:")
		slog.Warn("1. Copy the following Base64 encoded string:")
		fmt.Printf("\n%s\n\n", encoded) // Print clearly for copy-paste
		slog.Warn(fmt.Sprintf("2. Set the environment variable '%s' to this value in your Railway service settings.", h.tokenEnvVar))
		slog.Warn("3. Restart the application service on Railway.")
		slog.Warn("=======================")
		// We cannot set the env var programmatically, manual step is required.
		// The current in-memory token will be used until restart.
	}
	h.token = token // Update in-memory token
	return nil
}

func (h *OAuthHandler) loadToken() error {
	h.tokenMutex.Lock()
	defer h.tokenMutex.Unlock()

	var data []byte
	var err error

	if h.storageMethod == "file" {
		data, err = os.ReadFile(h.tokenFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return errors.New("token file not found")
			}
			return fmt.Errorf("failed to read token file '%s': %w", h.tokenFilePath, err)
		}
	} else if h.storageMethod == "env" {
		encoded := os.Getenv(h.tokenEnvVar)
		if encoded == "" {
			return fmt.Errorf("token env var '%s' is not set or empty", h.tokenEnvVar)
		}
		data, err = base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return fmt.Errorf("failed to decode token from env var '%s': %w", h.tokenEnvVar, err)
		}
	} else {
		return fmt.Errorf("unknown token storage method: %s", h.storageMethod)
	}

	var token oauth2.Token
	if err := json.Unmarshal(data, &token); err != nil {
		return fmt.Errorf("failed to unmarshal token: %w", err)
	}
	h.token = &token
	return nil
}

func (h *OAuthHandler) GetClient(ctx context.Context, cfg *Config) (*http.Client, error) {
	h.tokenMutex.Lock()
	token := h.token
	h.tokenMutex.Unlock()

	if token == nil {
		h.initialAuthLog.Do(func() {
			authURL := h.config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
			errMsg := fmt.Sprintf("Google API токен отсутствует или недействителен. Требуется авторизация.\n\n👉 Перейдите по ссылке для авторизации:\n%s\n\nПосле авторизации бот должен заработать.", authURL)
			h.feedback.NotifyAdminCritical("OAuth Token Missing", errors.New(errMsg))
		})
		return nil, errors.New("OAuth token is missing, authorization required")
	}

	tokenSource := h.config.TokenSource(ctx, token)
	newToken, err := tokenSource.Token() // This automatically refreshes if necessary
	if err != nil {
		// If refresh fails, the original token might be invalid (e.g., revoked)
		h.feedback.NotifyAdminCritical("OAuth Token Refresh Failed", fmt.Errorf("failed to refresh token: %w. Manual re-authorization might be needed.", err))

		// Trigger auth URL message again if refresh fails persistently
		authURL := h.config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
		errMsg := fmt.Sprintf("Не удалось обновить Google API токен. Возможно, он был отозван. Требуется повторная авторизация.\n\n👉 Перейдите по ссылке:\n%s", authURL)
		h.feedback.NotifyAdminCritical("OAuth Re-authorization Needed", errors.New(errMsg))

		h.tokenMutex.Lock()
		h.token = nil // Invalidate in-memory token
		h.tokenMutex.Unlock()

		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	// Check if the token was actually refreshed and needs saving
	h.tokenMutex.Lock()
	tokenNeedsSave := false
	if h.token == nil || newToken.AccessToken != h.token.AccessToken {
		tokenNeedsSave = true
		h.token = newToken // Update in-memory token immediately
	}
	h.tokenMutex.Unlock() // Unlock before potentially saving

	if tokenNeedsSave {
		if err := h.saveToken(newToken); err != nil {
			// Log error but continue, as we have a valid token in memory
			slog.Error("Failed to save refreshed token", "error", err)
			h.feedback.NotifyAdminFailure(0, 0, "System", nil, "Save Refreshed Token", err)
		} else {
			slog.Info("OAuth token refreshed and saved successfully")
		}
	}

	// Return client using the potentially updated token source
	return oauth2.NewClient(ctx, h.config.TokenSource(ctx, newToken)), nil
}

func (h *OAuthHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("state") != oauthState {
		http.Error(w, "Invalid state parameter", http.StatusBadRequest)
		h.feedback.NotifyAdminCritical("OAuth Callback Error", errors.New("invalid state parameter received"))
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "Code not found", http.StatusBadRequest)
		h.feedback.NotifyAdminCritical("OAuth Callback Error", errors.New("code parameter not found"))
		return
	}

	token, err := h.config.Exchange(r.Context(), code)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to exchange code for token: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		h.feedback.NotifyAdminCritical("OAuth Exchange Error", errors.New(errMsg))
		return
	}

	if err := h.saveToken(token); err != nil {
		errMsg := fmt.Sprintf("Failed to save initial token: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		h.feedback.NotifyAdminCritical("OAuth Save Initial Token Error", errors.New(errMsg))
		return
	}

	slog.Info("OAuth authorization successful, token saved.")
	fmt.Fprintln(w, "Авторизация прошла успешно! 🎉 Можете закрыть это окно.")
	h.feedback.NotifyAdminSuccess(&ParsedData{Username: "System"}, "OAuth Успешно", 1, 1)

}

func (h *OAuthHandler) GetAuthURL() string {
	// TODO: Generate and store a random state for CSRF protection
	return h.config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
}

// --- Drive Service ---

type DriveService struct {
	srv        *drive.Service
	oauth      *OAuthHandler
	cfg        *Config
	httpClient *http.Client // Store the client used to create the service
	clientMu   sync.RWMutex
}

func NewDriveService(ctx context.Context, oauth *OAuthHandler, cfg *Config) (*DriveService, error) {
	client, err := oauth.GetClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial OAuth client for Drive: %w", err)
	}
	srv, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create Drive service: %w", err)
	}
	return &DriveService{srv: srv, oauth: oauth, cfg: cfg, httpClient: client}, nil
}

// getService tries to return the current service, refreshing if the client seems invalid
func (s *DriveService) getService(ctx context.Context) (*drive.Service, error) {
	s.clientMu.RLock()
	currentSrv := s.srv
	currentClient := s.httpClient
	s.clientMu.RUnlock()

	// Attempt to get a potentially refreshed client from OAuthHandler
	// This doesn't guarantee the token is valid, but ensures we use the latest known token
	refreshedClient, err := s.oauth.GetClient(ctx, s.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get OAuth client for Drive: %w", err)
	}

	// If the client provided by OAuthHandler is different, update the service
	if refreshedClient != currentClient {
		return s.refreshService(ctx) // refreshService handles locking
	}

	return currentSrv, nil
}

// refreshService explicitly gets a new client and updates the service instance
func (s *DriveService) refreshService(ctx context.Context) (*drive.Service, error) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	client, err := s.oauth.GetClient(ctx, s.cfg) // Get potentially refreshed client
	if err != nil {
		return nil, fmt.Errorf("failed to get refreshed OAuth client for Drive: %w", err)
	}

	// Only create new service if the client actually changed
	// or if the service is nil (initial creation failed?)
	if s.srv == nil || client != s.httpClient {
		srv, err := drive.NewService(ctx, option.WithHTTPClient(client))
		if err != nil {
			return nil, fmt.Errorf("failed to create new Drive service after refresh: %w", err)
		}
		s.srv = srv
		s.httpClient = client
		slog.Info("Drive service client refreshed")
	}
	return s.srv, nil
}

func (s *DriveService) EnsureObjectFolder(ctx context.Context, parentFolderID, objectName string) (folderID, statusMsg string, err error) {
	sanitizedName := sanitizeFileName(strings.TrimSpace(objectName))
	if sanitizedName == "" {
		sanitizedName = "Разное"
	}

	query := fmt.Sprintf("name='%s' and mimeType='application/vnd.google-apps.folder' and '%s' in parents and trashed=false",
		strings.ReplaceAll(sanitizedName, "'", "\\'"), // Escape single quotes in name
		parentFolderID)

	var fl *drive.FileList
	var currentSrv *drive.Service

	for i := 0; i < s.cfg.MaxRetries; i++ {
		currentSrv, err = s.getService(ctx) // Use getService which attempts refresh if needed
		if err != nil {
			if i == s.cfg.MaxRetries-1 {
				return "", "", fmt.Errorf("failed to get drive service after %d retries: %w", s.cfg.MaxRetries, err)
			}
			time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
			continue
		}

		fl, err = currentSrv.Files.List().Q(query).Fields("files(id)").PageSize(1).Do()
		if err == nil {
			if len(fl.Files) > 0 {
				return fl.Files[0].Id, "Использована существующая папка", nil
			}
			// Folder not found, break loop to create it
			break
		}

		slog.Warn("Error listing drive folder, attempting retry", "attempt", i+1, "error", err)
		// No explicit refresh here, rely on getService in the next iteration
		// or the create block to handle potential token issues revealed by the error.
		time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
	}

	// Handle case where list succeeded but folder wasn't found OR list failed after retries
	if err != nil && (fl == nil || len(fl.Files) == 0) { // Check if err exists ONLY if files are empty
		return "", "", fmt.Errorf("failed to find drive folder '%s' after %d retries: %w", sanitizedName, s.cfg.MaxRetries, err)
	}

	// Create folder if not found
	folderMetadata := &drive.File{
		Name:     sanitizedName,
		Parents:  []string{parentFolderID},
		MimeType: "application/vnd.google-apps.folder",
	}

	var createdFolder *drive.File
	for i := 0; i < s.cfg.MaxRetries; i++ {
		currentSrv, err = s.getService(ctx) // Get potentially refreshed service again
		if err != nil {
			if i == s.cfg.MaxRetries-1 {
				return "", "", fmt.Errorf("failed to get drive service before create after %d retries: %w", s.cfg.MaxRetries, err)
			}
			time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
			continue
		}

		createdFolder, err = currentSrv.Files.Create(folderMetadata).Fields("id").Do()
		if err == nil {
			return createdFolder.Id, "Создана новая папка", nil
		}

		slog.Warn("Error creating drive folder, attempting retry", "attempt", i+1, "error", err)
		time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
	}

	return "", "", fmt.Errorf("failed to create drive folder '%s' after %d retries: %w", sanitizedName, s.cfg.MaxRetries, err)
}

func (s *DriveService) DownloadAndUploadFile(ctx context.Context, downloadURL, driveFileName, targetFolderID string) (webViewLink string, err error) {
	// 1. Download
	dlCtx, dlCancel := context.WithTimeout(ctx, 30*time.Second) // Timeout for download
	defer dlCancel()
	req, err := http.NewRequestWithContext(dlCtx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create download request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req) // Use default client
	if err != nil {
		return "", fmt.Errorf("failed to start download from telegram: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to download from telegram, status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// It's often better to stream directly to Drive API if possible,
	// but saving to temp file is simpler for retries.
	tmpFile, err := os.CreateTemp("", "telegram_photo_*.jpg")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up temp file

	_, err = io.Copy(tmpFile, resp.Body)
	tmpFile.Close() // Close immediately after copy or error
	if err != nil {
		return "", fmt.Errorf("failed to copy downloaded content to temp file: %w", err)
	}
	// Temp file closed here

	// 2. Upload with Retries
	var driveFile *drive.File
	var currentSrv *drive.Service

	for i := 0; i < s.cfg.MaxRetries; i++ {
		fileReader, err := os.Open(tmpFile.Name())
		if err != nil {
			return "", fmt.Errorf("failed to open temp file for upload attempt %d: %w", i+1, err)
		}
		// Defer close inside the loop for each attempt's reader
		closeReader := func(r io.Closer) {
			if r != nil {
				r.Close()
			}
		}
		defer closeReader(fileReader)

		currentSrv, err = s.getService(ctx)
		if err != nil {
			if i == s.cfg.MaxRetries-1 {
				return "", fmt.Errorf("failed to get drive service before upload after %d retries: %w", s.cfg.MaxRetries, err)
			}
			time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
			continue
		}

		fileMetadata := &drive.File{
			Name:    driveFileName,
			Parents: []string{targetFolderID},
		}

		// Context for the upload API call itself
		uploadCtx, uploadCancel := context.WithTimeout(ctx, 2*time.Minute) // Timeout for upload call

		driveFile, err = currentSrv.Files.Create(fileMetadata).Media(fileReader).Fields("id,webViewLink").Context(uploadCtx).Do()
		uploadCancel() // Cancel context as soon as Do returns

		// Close reader after Do() call for this attempt
		fileReader.Close() // Explicit close after use

		if err == nil {
			return driveFile.WebViewLink, nil // Success!
		}

		slog.Warn("Error uploading to drive, attempting retry", "attempt", i+1, "error", err)
		// Rely on getService in the next iteration to handle token issues.
		time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
	}

	return "", fmt.Errorf("failed to upload file '%s' to drive after %d retries: %w", driveFileName, s.cfg.MaxRetries, err)
}

// --- Sheets Service ---

type SheetsService struct {
	srv        *sheets.Service
	oauth      *OAuthHandler
	cfg        *Config
	httpClient *http.Client
	clientMu   sync.RWMutex
}

func NewSheetsService(ctx context.Context, oauth *OAuthHandler, cfg *Config) (*SheetsService, error) {
	client, err := oauth.GetClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial OAuth client for Sheets: %w", err)
	}
	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create Sheets service: %w", err)
	}
	return &SheetsService{srv: srv, oauth: oauth, cfg: cfg, httpClient: client}, nil
}

// getService tries to return the current service, refreshing if the client seems invalid
func (s *SheetsService) getService(ctx context.Context) (*sheets.Service, error) {
	s.clientMu.RLock()
	currentSrv := s.srv
	currentClient := s.httpClient
	s.clientMu.RUnlock()

	refreshedClient, err := s.oauth.GetClient(ctx, s.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get OAuth client for Sheets: %w", err)
	}

	if refreshedClient != currentClient {
		return s.refreshService(ctx)
	}

	return currentSrv, nil
}

// refreshService explicitly gets a new client and updates the service instance
func (s *SheetsService) refreshService(ctx context.Context) (*sheets.Service, error) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	client, err := s.oauth.GetClient(ctx, s.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get refreshed OAuth client for Sheets: %w", err)
	}

	if s.srv == nil || client != s.httpClient {
		srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
		if err != nil {
			return nil, fmt.Errorf("failed to create new Sheets service after refresh: %w", err)
		}
		s.srv = srv
		s.httpClient = client
		slog.Info("Sheets service client refreshed")
	}
	return s.srv, nil
}

func (s *SheetsService) AppendCheckData(ctx context.Context, sheetID string, data *ParsedData) error {
	const sheetReadRange = "'Чеки'!B:B"      // Assuming column B is always populated for checks
	const sheetWriteRange = "'Чеки'!A%d:F%d" // Assuming columns A-F: Date, User, Addr, Amt, Cmnt, Link

	var currentSrv *sheets.Service
	var err error
	var resp *sheets.ValueRange

	// Find next empty row
	for i := 0; i < s.cfg.MaxRetries; i++ {
		currentSrv, err = s.getService(ctx)
		if err != nil {
			if i == s.cfg.MaxRetries-1 {
				return fmt.Errorf("failed to get sheets service before read after %d retries: %w", s.cfg.MaxRetries, err)
			}
			time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
			continue
		}

		resp, err = currentSrv.Spreadsheets.Values.Get(sheetID, sheetReadRange).Do()
		if err == nil {
			break // Success reading
		}

		slog.Warn("Error reading sheet for row count, attempting retry", "attempt", i+1, "error", err)
		time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
	}
	if err != nil {
		return fmt.Errorf("failed to read sheet '%s' after %d retries: %w", sheetID, s.cfg.MaxRetries, err)
	}

	nextRow := len(resp.Values) + 1 // +1 because sheets are 1-indexed and we want the next empty row

	// Prepare data for writing
	values := []interface{}{data.Date, data.Username, data.Address, data.Amount, data.Comment, data.DriveLink}
	valueRange := &sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         [][]interface{}{values},
	}
	writeRange := fmt.Sprintf(sheetWriteRange, nextRow, nextRow)

	// Write data with retries
	for i := 0; i < s.cfg.MaxRetries; i++ {
		currentSrv, err = s.getService(ctx) // Get potentially refreshed service
		if err != nil {
			if i == s.cfg.MaxRetries-1 {
				return fmt.Errorf("failed to get sheets service before write after %d retries: %w", s.cfg.MaxRetries, err)
			}
			time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
			continue
		}

		_, err = currentSrv.Spreadsheets.Values.Update(sheetID, writeRange, valueRange).ValueInputOption("USER_ENTERED").Do()
		if err == nil {
			return nil // Success writing
		}

		slog.Warn("Error writing to sheet, attempting retry", "attempt", i+1, "error", err)
		time.Sleep(s.cfg.RetryDelay * time.Duration(i+1))
	}

	return fmt.Errorf("failed to write to sheet '%s' after %d retries: %w", sheetID, s.cfg.MaxRetries, err)
}

// --- Processor ---

type Processor struct {
	parser     *Parser
	drive      *DriveService
	sheets     *SheetsService
	feedback   *FeedbackHandler
	bot        *tgbotapi.BotAPI // Needed for GetFileDirectURL
	cfg        *Config
	mediaCache sync.Map   // string (mediaGroupID) -> *MediaGroupCacheEntry
	cacheMu    sync.Mutex // To protect creation/deletion logic around sync.Map
}

func NewProcessor(parser *Parser, drive *DriveService, sheets *SheetsService, feedback *FeedbackHandler, bot *tgbotapi.BotAPI, cfg *Config) *Processor {
	p := &Processor{
		parser:   parser,
		drive:    drive,
		sheets:   sheets,
		feedback: feedback,
		bot:      bot,
		cfg:      cfg,
	}
	go p.cleanupExpiredMediaGroups()
	return p
}

func (p *Processor) HandleSinglePhoto(ctx context.Context, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userID := msg.From.ID
	username := getFullName(msg.From)

	if msg.Caption == "" {
		p.feedback.SendSimpleReply(chatID, "❗️ Пожалуйста, добавьте подпись к фото с адресом и суммой.")
		p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Parse Caption", errors.New("empty caption"))
		return
	}

	addr, amt, comm, err := p.parser.ParseMessage(msg.Caption)
	if err != nil {
		p.feedback.SendParsingError(chatID, err.Error())
		p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Parse Caption", err)
		return
	}
	if addr == "" || amt == "" {
		err := errors.New("адрес или сумма не найдены в подписи")
		p.feedback.SendParsingError(chatID, err.Error())
		p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Parse Caption", err)
		return
	}

	folderID, folderMsg, err := p.drive.EnsureObjectFolder(ctx, p.cfg.DriveFolderID, addr)
	if err != nil {
		p.feedback.SendError(chatID, "не удалось создать/найти папку объекта в Google Drive.")
		p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Ensure Drive Folder", err)
		return
	}

	bestPhoto := msg.Photo[len(msg.Photo)-1]
	fileURL, err := p.bot.GetFileDirectURL(bestPhoto.FileID)
	if err != nil {
		p.feedback.SendError(chatID, "не удалось получить ссылку на фото.")
		p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Get Telegram File URL", err)
		return
	}

	msk := time.FixedZone("MSK", 3*3600)
	dateStr := time.Now().In(msk).Format("020106")
	sanitizedAddr := sanitizeFileName(addr)
	sanitizedAmt := sanitizeFileName(strings.ReplaceAll(amt, ",", "."))
	fileName := fmt.Sprintf("%s_%s_%s_01.jpg", dateStr, sanitizedAddr, sanitizedAmt)

	link, err := p.drive.DownloadAndUploadFile(ctx, fileURL, fileName, folderID)
	if err != nil {
		p.feedback.SendError(chatID, "не удалось загрузить фото в Google Drive.")
		p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Upload to Drive", err)
		return
	}

	parsedData := &ParsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  username,
		Date:      time.Now().In(msk).Format("02.01.2006 15:04:05"),
		DriveLink: link,
		ChatID:    chatID,
		UserID:    userID,
		SourceMsg: msg,
	}

	if err := p.sheets.AppendCheckData(ctx, p.cfg.SpreadsheetID, parsedData); err != nil {
		p.feedback.SendError(chatID, "не удалось записать данные в таблицу.")
		p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Append to Sheet", err)
		return
	}

	p.feedback.SendSuccess(chatID, parsedData, 1, 1)
	p.feedback.NotifyAdminSuccess(parsedData, folderMsg, 1, 1)
}

func (p *Processor) HandleMediaGroupUpdate(ctx context.Context, msg *tgbotapi.Message) {
	groupID := msg.MediaGroupID
	chatID := msg.Chat.ID
	userID := msg.From.ID
	username := getFullName(msg.From)
	bestPhoto := msg.Photo[len(msg.Photo)-1]

	p.cacheMu.Lock() // Lock mutex before accessing sync.Map for creation logic
	entryUntyped, loaded := p.mediaCache.LoadOrStore(groupID, &MediaGroupCacheEntry{
		Files:        make(map[string]*tgbotapi.PhotoSize),
		ChatID:       chatID,
		UserID:       userID, // Assume first message user is the owner
		Username:     username,
		IsProcessing: false,
		LastUpdated:  time.Now(),
		SourceMsg:    msg, // Store first message for context
	})
	entry := entryUntyped.(*MediaGroupCacheEntry)

	if loaded { // Entry already existed
		p.cacheMu.Unlock() // Unlock after LoadOrStore if loaded
		entry.LastUpdated = time.Now()
		// Use FileUniqueID if available and different, otherwise FileID
		fileID := bestPhoto.FileID
		if bestPhoto.FileUniqueID != "" {
			fileID = bestPhoto.FileUniqueID // More reliable for identifying duplicates
		}

		if _, exists := entry.Files[fileID]; !exists {
			entry.Files[fileID] = &bestPhoto
			if entry.ProcessTimer != nil {
				entry.ProcessTimer.Reset(p.cfg.MediaGroupTimeout)
			}
		}
		// Update caption/parsed data if empty and new message has caption
		if msg.Caption != "" && (entry.Address == "" || entry.Amount == "") {
			addr, amt, comm, err := p.parser.ParseMessage(msg.Caption)
			if err == nil && (addr != "" || amt != "") { // Only update if parsing is successful
				entry.Address = addr
				entry.Amount = amt
				entry.Comment = comm
				entry.Caption = msg.Caption
			}
		}
	} else { // New entry was stored
		// Parse caption from the first message
		if msg.Caption != "" {
			addr, amt, comm, err := p.parser.ParseMessage(msg.Caption)
			if err == nil {
				entry.Address = addr
				entry.Amount = amt
				entry.Comment = comm
				entry.Caption = msg.Caption
			} else {
				// Send error immediately for the first message if caption is bad
				p.feedback.SendParsingError(chatID, err.Error())
				p.feedback.NotifyAdminFailure(chatID, userID, username, msg, "Parse Initial Media Group Caption", err)
				// Don't start timer if initial parsing fails
				p.cacheMu.Unlock()
				return
			}
		}
		fileID := bestPhoto.FileID
		if bestPhoto.FileUniqueID != "" {
			fileID = bestPhoto.FileUniqueID
		}
		entry.Files[fileID] = &bestPhoto
		entry.ProcessTimer = time.AfterFunc(p.cfg.MediaGroupTimeout, func() {
			p.ProcessMediaGroup(context.Background(), groupID) // Use background context for timer
		})
		p.cacheMu.Unlock() // Unlock after creating new entry and timer
	}
}

func (p *Processor) ProcessMediaGroup(ctx context.Context, groupID string) {
	entryUntyped, loaded := p.mediaCache.Load(groupID)
	if !loaded {
		slog.Debug("ProcessMediaGroup called but entry not found (likely already processed)", "groupID", groupID)
		return // Already processed or expired
	}
	entry := entryUntyped.(*MediaGroupCacheEntry)

	// Prevent double processing
	p.cacheMu.Lock()
	if entry.IsProcessing {
		p.cacheMu.Unlock()
		slog.Debug("ProcessMediaGroup called but entry is already processing", "groupID", groupID)
		return
	}
	entry.IsProcessing = true
	// Delete immediately after marking as processing to prevent reprocessing on cleanup race condition
	p.mediaCache.Delete(groupID)
	p.cacheMu.Unlock()

	chatID := entry.ChatID
	userID := entry.UserID
	username := entry.Username

	if entry.Address == "" || entry.Amount == "" {
		p.feedback.SendSimpleReply(chatID, fmt.Sprintf("❗️ Не удалось найти адрес или сумму для группы фото [%s...]. Пожалуйста, добавьте подпись к первому фото.", groupID[:min(len(groupID), 5)]))
		p.feedback.NotifyAdminFailure(chatID, userID, username, entry.SourceMsg, "Process Media Group", errors.New("missing address or amount"))
		return
	}

	folderID, folderMsg, err := p.drive.EnsureObjectFolder(ctx, p.cfg.DriveFolderID, entry.Address)
	if err != nil {
		p.feedback.SendError(chatID, "не удалось создать/найти папку объекта в Google Drive.")
		p.feedback.NotifyAdminFailure(chatID, userID, username, entry.SourceMsg, "Ensure Drive Folder (Media Group)", err)
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, p.cfg.MaxConcurrentUploads)
	results := make(chan string, len(entry.Files))
	uploadErrors := make(chan error, len(entry.Files))
	photoCount := len(entry.Files)

	fileIndex := 0
	// Create a slice of photos to iterate deterministically
	photosToProcess := make([]*tgbotapi.PhotoSize, 0, len(entry.Files))
	for _, photo := range entry.Files {
		photosToProcess = append(photosToProcess, photo)
	}
	// Optional: Sort photos if order matters, e.g., by FileID or some timestamp if available
	// sort.Slice(photosToProcess, func(i, j int) bool { ... })

	for _, photo := range photosToProcess {
		wg.Add(1)
		fileIndex++
		go func(fIndex int, ph *tgbotapi.PhotoSize) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			fileURL, err := p.bot.GetFileDirectURL(ph.FileID)
			if err != nil {
				uploadErrors <- fmt.Errorf("фото %d: не удалось получить ссылку (%w)", fIndex, err)
				return
			}

			msk := time.FixedZone("MSK", 3*3600)
			dateStr := time.Now().In(msk).Format("020106")
			sanitizedAddr := sanitizeFileName(entry.Address)
			sanitizedAmt := sanitizeFileName(strings.ReplaceAll(entry.Amount, ",", "."))
			fileName := fmt.Sprintf("%s_%s_%s_%02d.jpg", dateStr, sanitizedAddr, sanitizedAmt, fIndex)

			link, err := p.drive.DownloadAndUploadFile(ctx, fileURL, fileName, folderID)
			if err != nil {
				uploadErrors <- fmt.Errorf("фото %d: не удалось загрузить (%w)", fIndex, err)
				return
			}
			results <- link
		}(fileIndex, photo)
	}

	wg.Wait()
	close(results)
	close(uploadErrors)

	var links []string
	for link := range results {
		links = append(links, link)
	}

	var combinedError error
	errorMessages := []string{}
	for err := range uploadErrors {
		errorMessages = append(errorMessages, err.Error())
	}
	if len(errorMessages) > 0 {
		combinedError = errors.New(strings.Join(errorMessages, "; "))
		p.feedback.NotifyAdminFailure(chatID, userID, username, entry.SourceMsg, "Upload Media Group Files", combinedError)
	}

	if len(links) == 0 {
		p.feedback.SendError(chatID, "не удалось загрузить ни одного фото из группы.")
		// Admin already notified above if there were errors
		return
	}

	msk := time.FixedZone("MSK", 3*3600)
	parsedData := &ParsedData{
		Address:   entry.Address,
		Amount:    entry.Amount,
		Comment:   entry.Comment,
		Username:  username,
		Date:      time.Now().In(msk).Format("02.01.2006 15:04:05"),
		DriveLink: strings.Join(links, " "),
		ChatID:    chatID,
		UserID:    userID,
		SourceMsg: entry.SourceMsg,
	}

	if err := p.sheets.AppendCheckData(ctx, p.cfg.SpreadsheetID, parsedData); err != nil {
		p.feedback.SendError(chatID, "не удалось записать данные группы в таблицу.")
		p.feedback.NotifyAdminFailure(chatID, userID, username, entry.SourceMsg, "Append Media Group to Sheet", err)
		// Also notify about partial success maybe?
		if len(errorMessages) == 0 { // If uploads were fine, but sheet failed
			p.feedback.NotifyAdminSuccess(parsedData, folderMsg, len(links), photoCount) // Notify success for drive part
		}
		return
	}

	p.feedback.SendSuccess(chatID, parsedData, len(links), photoCount)
	p.feedback.NotifyAdminSuccess(parsedData, folderMsg, len(links), photoCount)
	if combinedError != nil {
		// Send simplified error to user
		p.feedback.SendSimpleReply(chatID, fmt.Sprintf("❗️ Были ошибки при загрузке некоторых фото (подробности у администратора)."))
	}
}

func (p *Processor) cleanupExpiredMediaGroups() {
	ticker := time.NewTicker(5 * time.Minute) // Check every 5 minutes
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		p.mediaCache.Range(func(key, value interface{}) bool {
			entry := value.(*MediaGroupCacheEntry)
			// Remove groups older than 10 minutes that are not processing
			if !entry.IsProcessing && now.Sub(entry.LastUpdated) > 10*time.Minute {
				p.mediaCache.Delete(key)
				slog.Debug("Cleaned up expired media group cache entry", "groupID", key)
			}
			return true
		})
	}
}

// --- Telegram Handler ---

type TelegramHandler struct {
	bot       *tgbotapi.BotAPI
	processor *Processor
	feedback  *FeedbackHandler
	oauth     *OAuthHandler
	cfg       *Config
}

func NewTelegramHandler(bot *tgbotapi.BotAPI, processor *Processor, feedback *FeedbackHandler, oauth *OAuthHandler, cfg *Config) *TelegramHandler {
	return &TelegramHandler{
		bot:       bot,
		processor: processor,
		feedback:  feedback,
		oauth:     oauth,
		cfg:       cfg,
	}
}

func (h *TelegramHandler) RegisterHandlers(mux *http.ServeMux) {
	// Use the specific webhook path from config if needed, or constant
	fullWebhookPath := "/" + strings.TrimPrefix(webhookPath, "/")
	mux.HandleFunc(fullWebhookPath, h.handleWebhook)

	fullCallbackPath := "/" + strings.TrimPrefix(oauthCallbackPath, "/")
	mux.HandleFunc(fullCallbackPath, h.oauth.HandleCallback)

	fullAuthPath := "/" + strings.TrimPrefix(authPath, "/")
	mux.HandleFunc(fullAuthPath, h.handleAuthRedirect)
}

func (h *TelegramHandler) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read webhook body", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	r.Body.Close() // Close body after reading

	var update tgbotapi.Update
	if err := json.Unmarshal(bytes, &update); err != nil {
		slog.Error("Failed to unmarshal webhook update", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// Respond quickly to Telegram
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "{}") // Send empty JSON response

	// Process update asynchronously
	// Create a new context for the background processing
	procCtx, procCancel := context.WithTimeout(context.Background(), 5*time.Minute) // Timeout for processing
	go func(ctx context.Context, u tgbotapi.Update) {
		defer procCancel()
		h.processUpdate(ctx, u)
	}(procCtx, update)
}

func (h *TelegramHandler) processUpdate(ctx context.Context, update tgbotapi.Update) {
	if update.Message == nil {
		return // Ignore other update types for now
	}

	msg := update.Message
	chatID := msg.Chat.ID

	// Basic check if services are available (they might fail init)
	if h.processor.drive == nil || h.processor.sheets == nil {
		h.feedback.SendError(chatID, "Сервис временно недоступен (ошибка инициализации). Обратитесь к администратору.")
		h.feedback.NotifyAdminCritical("Process Update Halted", errors.New("Drive or Sheets service is nil"))
		return
	}

	if msg.IsCommand() {
		switch msg.Command() {
		case "start", "help":
			h.sendHelpMessage(chatID)
		case "objects":
			h.sendObjectsList(chatID)
		default:
			h.feedback.SendSimpleReply(chatID, "Неизвестная команда 🤔")
		}
	} else if msg.Text == "Начать" || msg.Text == "Помощь (/help)" { // Handle button text too
		h.sendHelpMessage(chatID)
	} else if msg.Text == "Объекты" {
		h.sendObjectsList(chatID)
	} else if msg.Photo != nil && len(msg.Photo) > 0 {
		if msg.MediaGroupID != "" {
			h.processor.HandleMediaGroupUpdate(ctx, msg)
		} else {
			h.processor.HandleSinglePhoto(ctx, msg)
		}
	} else {
		// Optional: Handle unexpected text messages
		// h.feedback.SendSimpleReply(chatID, "Пожалуйста, отправьте фото чека с подписью или используйте команды /help, /objects.")
	}
}

func (h *TelegramHandler) handleAuthRedirect(w http.ResponseWriter, r *http.Request) {
	authURL := h.oauth.GetAuthURL()
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

func (h *TelegramHandler) sendHelpMessage(chatID int64) {
	helpText := `
👋 *Бот для учета чеков!*

Отправьте фото чека (или несколько фото) с подписью, указав:
• *Адрес* 🏠
• *Сумму* 💰
• *Комментарий* (необязательно) 📝

*Форматы подписей:*

1️⃣ *С ключевыми словами:*
   Адрес: Каскад 2 Сумма: 500.50 Комментарий: Лампочки

2️⃣ *По строкам:*
   Каскад 2
   500,50
   Лампочки

*Важно:*
✨ При отправке *нескольких фото* (медиа-группы), подпись добавляйте *только к первому фото*.
👍 Дождитесь ответа бота (✅ успех или ❗️ ошибка) перед отправкой следующего чека.
👉 Используйте команду /objects или кнопку "Объекты" для просмотра списка адресов.

Удачного учета! 😊
`
	msg := tgbotapi.NewMessage(chatID, helpText)
	msg.ParseMode = tgbotapi.ModeMarkdown
	msg.ReplyMarkup = h.createMainMenuKeyboard()
	h.feedback.send(msg) // Use feedback handler to send
}

func (h *TelegramHandler) sendObjectsList(chatID int64) {
	var builder strings.Builder
	builder.WriteString("📍 *Список объектов:*\n\n")
	for i, addr := range objectAddresses {
		builder.WriteString(fmt.Sprintf("%d. `%s`\n", i+1, addr)) // Use backticks for easy copy
	}
	builder.WriteString("\nВы можете скопировать нужный адрес.")
	msg := tgbotapi.NewMessage(chatID, builder.String())
	msg.ParseMode = tgbotapi.ModeMarkdown
	h.feedback.send(msg)
}

func (h *TelegramHandler) createMainMenuKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Объекты"),
			tgbotapi.NewKeyboardButton("Помощь (/help)"),
		),
	)
}

func getFullName(user *tgbotapi.User) string {
	if user == nil {
		return "Unknown User"
	}
	name := user.FirstName
	if user.LastName != "" {
		name += " " + user.LastName
	}
	if user.UserName != "" {
		name += fmt.Sprintf(" (@%s)", user.UserName)
	}
	return name
}

// --- Main Application ---

func main() {
	// Use structured logging
	logLevel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		logLevel = slog.LevelDebug
	}
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	cfg, err := LoadConfig()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	bot, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		slog.Error("Failed to initialize Telegram Bot API", "error", err)
		os.Exit(1)
	}
	slog.Info("Telegram Bot API initialized", "username", bot.Self.UserName)

	// Setup Webhook
	fullWebhookURL := cfg.WebhookURL // Use the full URL from env var directly
	if !strings.HasSuffix(fullWebhookURL, webhookPath) {
		// Optional: Warn or error if WEBHOOK_URL doesn't end with expected path?
		// For now, assume WEBHOOK_URL is the correct, final URL Telegram should hit.
		slog.Warn("WEBHOOK_URL does not end with the expected path", "webhook_url", cfg.WebhookURL, "expected_suffix", webhookPath)
	}

	webhookConfig, err := tgbotapi.NewWebhook(fullWebhookURL)
	if err != nil {
		slog.Error("Failed to create webhook config", "error", err)
		os.Exit(1)
	}
	webhookConfig.AllowedUpdates = []string{"message"} // Only listen for messages
	_, err = bot.Request(webhookConfig)
	if err != nil {
		slog.Error("Failed to set webhook", "error", err)
		os.Exit(1)
	}
	// Verify webhook setup
	webhookInfo, err := bot.GetWebhookInfo()
	if err != nil {
		slog.Error("Failed to get webhook info", "error", err)
		os.Exit(1)
	}
	// Compare the string representation of the configured URL with the one reported by Telegram
	if webhookInfo.URL != webhookConfig.URL.String() { // CORRECTED COMPARISON
		slog.Error("Webhook URL mismatch", "expected", webhookConfig.URL.String(), "actual", webhookInfo.URL)
		// Don't exit here, maybe Telegram modifies the URL slightly (e.g. trailing slash)
		// Log as warning instead.
		slog.Warn("Webhook URL mismatch detected, check Telegram settings if issues arise.")
		// os.Exit(1)
	} else {
		slog.Info("Telegram webhook set successfully", "url", webhookInfo.URL)
	}

	feedback := NewFeedbackHandler(bot, cfg.AdminID)
	oauthHandler, err := NewOAuthHandler(cfg, feedback)
	if err != nil {
		slog.Warn("OAuth handler initialization potentially incomplete", "error", err)
	}

	parser := NewParser()
	ctx := context.Background()

	// Initialize services, handling potential nil returns if OAuth fails early
	var driveService *DriveService
	var sheetsService *SheetsService

	driveService, err = NewDriveService(ctx, oauthHandler, cfg)
	if err != nil {
		feedback.NotifyAdminCritical("Drive Service Init Failed", err)
		slog.Error("Drive Service initialization failed", "error", err)
		// driveService will be nil, processor needs to handle this
	}
	sheetsService, err = NewSheetsService(ctx, oauthHandler, cfg)
	if err != nil {
		feedback.NotifyAdminCritical("Sheets Service Init Failed", err)
		slog.Error("Sheets Service initialization failed", "error", err)
		// sheetsService will be nil
	}

	processor := NewProcessor(parser, driveService, sheetsService, feedback, bot, cfg)
	telegramHandler := NewTelegramHandler(bot, processor, feedback, oauthHandler, cfg)

	mux := http.NewServeMux()
	telegramHandler.RegisterHandlers(mux)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second, // Increased slightly
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  75 * time.Second,
	}

	// Graceful Shutdown
	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint

		slog.Info("Received interrupt signal, shutting down gracefully...")

		// Remove webhook before shutting down server
		if _, err := bot.Request(tgbotapi.DeleteWebhookConfig{DropPendingUpdates: false}); err != nil {
			slog.Error("Failed to delete webhook", "error", err)
		} else {
			slog.Info("Webhook deleted successfully.")
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Increased shutdown timeout
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server Shutdown failed", "error", err)
		}
		slog.Info("HTTP server shutdown complete.")
		close(idleConnsClosed)
	}()

	slog.Info("Starting HTTP server", "port", cfg.Port)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("HTTP server ListenAndServe failed", "error", err)
		feedback.NotifyAdminCritical("HTTP Server Failed", err)
		os.Exit(1)
	}

	<-idleConnsClosed
	slog.Info("Application shut down complete.")
}
