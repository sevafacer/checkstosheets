package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
	"github.com/nao1215/markdown"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const (
	maxRetries           = 3
	retryDelay           = 2
	tokenRefreshWindow   = 5 * time.Minute
	maxConcurrentUploads = 5

	sheetIDRange = "'Чеки'!B:B"
	sheetUpdate  = "'Чеки'!B%d:G%d"
	tokenFile    = "token.json"
)

var (
	fieldKeywords = map[string][]string{
		"address": {"адрес", "объект", "квартира", "школа", "дом", "улица", "место", "локация"},
		"amount":  {"сумма", "стоимость", "оплата", "платёж", "цена"},
		"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение", "заметка"},
	}

	tokenMutex        sync.Mutex
	mediaGroupCacheMu sync.Mutex

	oauthConfig     *oauth2.Config
	oauthState      = "state-token"
	authCodeCh      = make(chan string)
	mediaGroupCache = make(map[string]*MediaGroupData)
)

type ParsedData struct {
	Address, Amount, Comment, Username, Date, DriveLink string
}

type MediaGroupData struct {
	Files                             map[string]*tgbotapi.PhotoSize
	Caption, Address, Amount, Comment string
	Username                          string
	FirstMessageTime, LastUpdated     time.Time
	UserID, ChatID                    int64
	IsProcessing                      bool
}

type fieldMatch struct {
	field      string
	start, end int
}

func saveTokenToFile(token *oauth2.Token) error {
	data, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("ошибка маршалинга токена: %v", err)
	}
	return os.WriteFile(tokenFile, data, 0600)
}

func loadTokenFromFile() (*oauth2.Token, error) {
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}
	var token oauth2.Token
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, err
	}
	return &token, nil
}

// ==========================
// Конфигурация и инициализация
// ==========================
func loadEnvVars() (telegramToken, spreadsheetID, driveFolderID string, adminID int64, googleClientID, googleClientSecret, webhookURL string) {
	telegramToken = os.Getenv("TELEGRAM_BOT_TOKEN")
	spreadsheetID = os.Getenv("GOOGLE_SHEET_ID")
	driveFolderID = os.Getenv("GOOGLE_DRIVE_FOLDER_ID")
	adminStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	googleClientID = os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret = os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	webhookURL = os.Getenv("WEBHOOK_URL")

	if telegramToken == "" || spreadsheetID == "" || driveFolderID == "" || adminStr == "" ||
		googleClientID == "" || googleClientSecret == "" || webhookURL == "" {
		log.Fatal("Одна или несколько обязательных переменных окружения не установлены ❌")
	}
	var err error
	adminID, err = strconv.ParseInt(adminStr, 10, 64)
	if err != nil {
		log.Fatalf("Неверный формат ADMIN_CHAT_ID: %v", err)
	}
	return
}

// ==========================
// OAuth функции
// ==========================
func getOAuthClient(config *oauth2.Config) (*http.Client, error) {
	tokenMutex.Lock()
	token, err := loadTokenFromFile()
	tokenMutex.Unlock()

	if err == nil && token.Valid() {
		if time.Until(token.Expiry) < tokenRefreshWindow && token.RefreshToken != "" {
			if newToken, err := refreshToken(config, token); err == nil {
				_ = saveTokenToFile(newToken)
				return config.Client(context.Background(), newToken), nil
			}
		}
		return config.Client(context.Background(), token), nil
	}

	errCh := make(chan error, 1)
	server := startOAuthServer(errCh)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()
	authURL := config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	// Выводим ссылку для авторизации, чтобы пользователь мог перейти по ней
	fmt.Printf("👉 Перейдите по ссылке для авторизации:\n%s\n", authURL)
	select {
	case code := <-authCodeCh:
		token, err := config.Exchange(context.Background(), code)
		if err != nil {
			return nil, fmt.Errorf("ошибка обмена кода: %v", err)
		}
		_ = saveTokenToFile(token)
		return config.Client(context.Background(), token), nil
	case err := <-errCh:
		return nil, fmt.Errorf("ошибка OAuth сервера: %v", err)
	case <-time.After(5 * time.Minute):
		return nil, errors.New("превышено время ожидания авторизации ⏰")
	}
}

func startOAuthServer(errCh chan error) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != oauthState {
			http.Error(w, "Неверный state", http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "Код не найден", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "Авторизация прошла успешно. Закройте окно 😊")
		authCodeCh <- code
	})
	server := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	return server
}

func refreshToken(config *oauth2.Config, token *oauth2.Token) (*oauth2.Token, error) {
	var newToken *oauth2.Token
	var err error
	for i := 0; i < maxRetries; i++ {
		newToken, err = config.TokenSource(context.Background(), token).Token()
		if err == nil {
			if newToken.RefreshToken == "" {
				newToken.RefreshToken = token.RefreshToken
			}
			return newToken, nil
		}
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return nil, fmt.Errorf("не удалось обновить токен после %d попыток: %v", maxRetries, err)
}

// ==========================
// Функции работы с Google Drive
// ==========================
func ensureObjectFolder(srv *drive.Service, parentID, name string) (string, string, error) {
	name = sanitizeFileName(strings.TrimSpace(name))
	if name == "" {
		name = "Разное"
	}
	query := fmt.Sprintf("name='%s' and mimeType='application/vnd.google-apps.folder' and '%s' in parents and trashed=false", name, parentID)
	var fl *drive.FileList
	var err error
	for i := 0; i < maxRetries; i++ {
		fl, err = srv.Files.List().Q(query).Fields("files(id)").PageSize(10).Do()
		if err == nil {
			break
		}
		srv, _ = refreshDriveService(srv, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	if err != nil {
		return "", "", fmt.Errorf("поиск папки не удался: %v", err)
	}
	if len(fl.Files) > 0 {
		return fl.Files[0].Id, "Чек загружен в существующую папку", nil
	}
	// Создаём новую папку
	folder := &drive.File{
		Name:     name,
		Parents:  []string{parentID},
		MimeType: "application/vnd.google-apps.folder",
	}
	var created *drive.File
	for i := 0; i < maxRetries; i++ {
		created, err = srv.Files.Create(folder).Fields("id").Do()
		if err == nil {
			break
		}
		srv, _ = refreshDriveService(srv, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	if err != nil {
		return "", "", fmt.Errorf("создание папки не удалось: %v", err)
	}
	return created.Id, "Создана новая папка и чек загружен туда", nil
}

func refreshDriveService(srv *drive.Service, origErr error) (*drive.Service, error) {
	if strings.Contains(origErr.Error(), "oauth2: token expired") {
		newClient, err := getOAuthClient(oauthConfig)
		if err != nil {
			return srv, fmt.Errorf("обновление клиента не удалось: %v", err)
		}
		newSrv, err := drive.NewService(context.Background(), option.WithHTTPClient(newClient))
		if err != nil {
			return srv, fmt.Errorf("создание нового сервиса не удалось: %v", err)
		}
		return newSrv, nil
	}
	return srv, origErr
}

func downloadAndUploadFile(fileURL, fileName string, driveSrv *drive.Service, folderID string) (string, error) {
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("❗️ Ошибка скачивания: %v", err)
	}
	defer resp.Body.Close()

	tmpFile, err := os.CreateTemp("", "tg_photo_*")
	if err != nil {
		return "", fmt.Errorf("❗️ Ошибка создания temp файла: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err = io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("❗️ Ошибка копирования: %v", err)
	}
	tmpFile.Close()

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		file, err := os.Open(tmpFile.Name())
		if err != nil {
			return "", fmt.Errorf("❗️ Ошибка открытия файла: %v", err)
		}
		driveFile := &drive.File{
			Name:    fileName,
			Parents: []string{folderID},
		}
		res, err := driveSrv.Files.Create(driveFile).Media(file).Fields("webViewLink").Do()
		file.Close()
		if err == nil {
			return res.WebViewLink, nil
		}
		lastErr = err
		driveSrv, _ = refreshDriveService(driveSrv, err)
		time.Sleep(time.Duration(retryDelay*(i+1)) * time.Second)
	}
	return "", fmt.Errorf("❗️ Загрузка файла не удалась после %d попыток: %v", maxRetries, lastErr)
}

func uploadPhotoToDrive(driveSrv *drive.Service, fileURL, parentID, filename string) (string, error) {
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	driveFile := &drive.File{
		Name:     filename,
		MimeType: "image/jpeg",
	}
	if parentID != "" {
		driveFile.Parents = []string{parentID}
	}
	file, err := driveSrv.Files.Create(driveFile).Media(resp.Body).Do()
	if err != nil {
		return "", err
	}
	return file.Id, nil
}

// ==========================
// Функции работы с Google Sheets
// ==========================
func appendToSheet(srv *sheets.Service, sheetID string, data ParsedData) error {
	values := []interface{}{data.Date, data.Username, data.Address, data.Amount, data.Comment, data.DriveLink}
	vr := &sheets.ValueRange{Values: [][]interface{}{values}}

	resp, err := srv.Spreadsheets.Values.Get(sheetID, sheetIDRange).Do()
	if err != nil {
		return fmt.Errorf("получение данных не удалось: %v", err)
	}
	row := len(resp.Values) + 1
	_, err = srv.Spreadsheets.Values.Update(sheetID, fmt.Sprintf(sheetUpdate, row, row), vr).
		ValueInputOption("USER_ENTERED").Do()
	return err
}

// ==========================
// Уведомления для администратора
// ==========================
func notifyAdminSuccess(bot *tgbotapi.BotAPI, adminID int64, data ParsedData, userMsg *tgbotapi.Message, folderMsg string) {
	var origMsg string
	if userMsg != nil {
		origMsg = userMsg.Text
	}
	msgText := fmt.Sprintf("✅ Чек успешно загружен!\n\nПользователь: %s\nВремя: %s\nАдрес: %s\nСумма: %s\nКомментарий: %s\nСсылка на файл: %s\nПапка: %s\n\nИсходное сообщение:\n%s",
		data.Username, data.Date, data.Address, data.Amount, data.Comment, data.DriveLink, folderMsg, origMsg)
	adminMsg := tgbotapi.NewMessage(adminID, msgText)
	bot.Send(adminMsg)
}

func notifyAdminFailure(bot *tgbotapi.BotAPI, adminID int64, err error, userMsg *tgbotapi.Message) {
	var origMsg, userName string
	if userMsg != nil {
		origMsg = userMsg.Text
		userName = getFullName(userMsg.From)
	}
	msgText := fmt.Sprintf("❗️ Ошибка обработки чека!\n\nПользователь: %s\nИсходное сообщение:\n%s\n\nОшибка: %v",
		userName, origMsg, err)
	adminMsg := tgbotapi.NewMessage(adminID, msgText)
	bot.Send(adminMsg)
}

// ==========================
// Функции парсинга и вспомогательные
// ==========================
func parseMessage(message string) (string, string, string, error) {
	if strings.TrimSpace(message) == "" {
		return "", "", "", errors.New("пустое сообщение")
	}
	if strings.ContainsAny(message, ":=") {
		normalized := strings.Join(strings.Fields(message), " ")
		var matches []fieldMatch
		for field, kws := range fieldKeywords {
			for _, kw := range kws {
				pattern := fmt.Sprintf("(?i)%s\\s*[:=]\\s*", regexp.QuoteMeta(kw))
				re := regexp.MustCompile(pattern)
				for _, loc := range re.FindAllStringIndex(normalized, -1) {
					matches = append(matches, fieldMatch{field: field, start: loc[0], end: loc[1]})
				}
			}
		}
		if len(matches) > 0 {
			sort.Slice(matches, func(i, j int) bool { return matches[i].start < matches[j].start })
			fieldValues := make(map[string]string)
			for i, m := range matches {
				endPos := len(normalized)
				if i < len(matches)-1 {
					endPos = matches[i+1].start
				}
				val := strings.TrimSpace(normalized[m.end:endPos])
				if _, ok := fieldValues[m.field]; !ok && val != "" {
					fieldValues[m.field] = val
				}
			}
			addr, amt := fieldValues["address"], cleanAmount(fieldValues["amount"])
			comm := fieldValues["comment"]
			if addr == "" || amt == "" {
				return fallbackParse(message)
			}
			return addr, amt, comm, nil
		}
		return fallbackParse(message)
	}
	return fallbackParse(message)
}

func fallbackParse(message string) (string, string, string, error) {
	if strings.Contains(message, "\n") {
		lines := strings.Split(message, "\n")
		addr := removeLeadingKeyword(strings.TrimSpace(lines[0]), fieldKeywords["address"])
		amt := removeLeadingKeyword(strings.TrimSpace(lines[1]), fieldKeywords["amount"])
		amt = cleanAmount(amt)
		comm := ""
		if len(lines) > 2 {
			comm = removeLeadingKeyword(strings.TrimSpace(strings.Join(lines[2:], "\n")), fieldKeywords["comment"])
		}
		if addr == "" || amt == "" {
			return "", "", "", errors.New("обязательные поля не найдены")
		}
		return addr, amt, comm, nil
	}
	lowerMsg := strings.ToLower(message)
	amountIdx := -1
	for _, kw := range fieldKeywords["amount"] {
		if idx := strings.Index(lowerMsg, kw); idx != -1 && (amountIdx == -1 || idx < amountIdx) {
			amountIdx = idx
		}
	}
	if amountIdx != -1 {
		addr := removeLeadingKeyword(strings.TrimSpace(message[:amountIdx]), fieldKeywords["address"])
		amountPart := removeLeadingKeyword(strings.TrimSpace(message[amountIdx:]), fieldKeywords["amount"])
		commentIdx := -1
		for _, kw := range fieldKeywords["comment"] {
			if idx := strings.Index(strings.ToLower(amountPart), kw); idx != -1 && (commentIdx == -1 || idx < commentIdx) {
				commentIdx = idx
			}
		}
		if commentIdx != -1 {
			amt := cleanAmount(strings.TrimSpace(amountPart[:commentIdx]))
			comm := removeLeadingKeyword(strings.TrimSpace(amountPart[commentIdx:]), fieldKeywords["comment"])
			return addr, amt, comm, nil
		}
		return addr, cleanAmount(amountPart), "", nil
	}
	return removeLeadingKeyword(message, fieldKeywords["address"]), "", "", errors.New("сумма не найдена")
}

func cleanAmount(amount string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	cleaned := re.ReplaceAllString(amount, "")
	return strings.ReplaceAll(cleaned, ".", ",")
}

func sanitizeFileName(name string) string {
	re := regexp.MustCompile(`[^а-яА-ЯёЁa-zA-Z0-9\s\.-]`)
	s := re.ReplaceAllString(name, "_")
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, "_")
	return strings.Trim(regexp.MustCompile(`_+`).ReplaceAllString(s, "_"), "_")
}

func removeLeadingKeyword(text string, keywords []string) string {
	trimmed := strings.TrimSpace(text)
	lower := strings.ToLower(trimmed)
	for _, kw := range keywords {
		if strings.HasPrefix(lower, kw) {
			return strings.TrimSpace(trimmed[len(kw):])
		}
	}
	return trimmed
}

func getFullName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

// ==========================
// Telegram Хендлеры
// ==========================
func handleSinglePhotoMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, sheetsSrv *sheets.Service, sheetID string, driveSrv *drive.Service, parentID string, adminID int64) {
	if msg.Caption == "" {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Укажи адрес и сумму в подписи к фото в формате:\nАдрес: ...\nСумма: ..."))
		return
	}
	addr, amt, comm, err := parseMessage(msg.Caption)
	if err != nil {
		notifyAdminFailure(bot, adminID, fmt.Errorf("не удалось распознать подпись: %v", err), msg)
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Не удалось распознать подпись. Укажи адрес и сумму в формате:\nАдрес: ...\nСумма: ..."))
		return
	}
	if addr == "" || amt == "" {
		notifyAdminFailure(bot, adminID, errors.New("адрес или сумма не указаны"), msg)
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Обязательно укажи адрес и сумму в подписи!"))
		return
	}
	folderID, folderMsg, err := ensureObjectFolder(driveSrv, parentID, addr)
	if err != nil {
		notifyAdminFailure(bot, adminID, fmt.Errorf("ошибка обработки объекта: %v", err), msg)
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Ошибка обработки объекта: "+err.Error()))
		return
	}
	best := msg.Photo[len(msg.Photo)-1]
	fileURL, err := bot.GetFileDirectURL(best.FileID)
	if err != nil {
		notifyAdminFailure(bot, adminID, fmt.Errorf("не удалось получить фото: %v", err), msg)
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Не удалось получить фото: "+err.Error()))
		return
	}
	fileID, err := uploadPhotoToDrive(driveSrv, fileURL, folderID, fmt.Sprintf("check_%s", time.Now().Format("20060102_150405")))
	if err != nil {
		notifyAdminFailure(bot, adminID, fmt.Errorf("не удалось загрузить фото в Google Drive: %v", err), msg)
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Не удалось загрузить фото в Google Drive: "+err.Error()))
		return
	}
	link := fmt.Sprintf("https://drive.google.com/file/d/%s/view", fileID)
	parsedData := ParsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  getFullName(msg.From),
		Date:      time.Now().Format("02.01.2006 15:04:05"),
		DriveLink: link,
	}
	if err := appendToSheet(sheetsSrv, sheetID, parsedData); err != nil {
		notifyAdminFailure(bot, adminID, fmt.Errorf("ошибка записи в таблицу: %v", err), msg)
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Не удалось записать данные в таблицу: "+err.Error()))
		return
	}
	bot.Send(tgbotapi.NewMessage(msg.Chat.ID, fmt.Sprintf("✅ Чек успешно загружен!\nАдрес: %s\nСумма: %s", addr, amt)))
	notifyAdminSuccess(bot, adminID, parsedData, msg, folderMsg)
}

func handleMediaGroupMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message, sheetsSrv *sheets.Service, sheetID string, driveSrv *drive.Service, parentID string, adminID int64) {
	if len(msg.Photo) == 0 {
		bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Сообщение не содержит фотографий."))
		return
	}
	if msg.MediaGroupID == "" {
		handleSinglePhotoMessage(bot, msg, sheetsSrv, sheetID, driveSrv, parentID, adminID)
		return
	}
	mediaGroupCacheMu.Lock()
	group, exists := mediaGroupCache[msg.MediaGroupID]
	if !exists {
		addr, amt, comm, err := parseMessage(msg.Caption)
		if err != nil {
			bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Не удалось распознать подпись. Укажи адрес и сумму в формате:\nАдрес: ...\nСумма: ..."))
			mediaGroupCacheMu.Unlock()
			return
		}
		if addr == "" || amt == "" {
			bot.Send(tgbotapi.NewMessage(msg.Chat.ID, "❗️ Обязательно укажи адрес и сумму в подписи к фотоальбому!"))
			mediaGroupCacheMu.Unlock()
			return
		}
		group = &MediaGroupData{
			Files:            make(map[string]*tgbotapi.PhotoSize),
			Caption:          msg.Caption,
			Address:          addr,
			Amount:           amt,
			Comment:          comm,
			FirstMessageTime: time.Now(),
			LastUpdated:      time.Now(),
			UserID:           msg.From.ID,
			ChatID:           msg.Chat.ID,
			Username:         getFullName(msg.From),
		}
		mediaGroupCache[msg.MediaGroupID] = group
	}
	best := msg.Photo[len(msg.Photo)-1]
	if _, ok := group.Files[best.FileID]; !ok {
		group.Files[best.FileID] = &best
		group.LastUpdated = time.Now()
	}
	shouldProcess := !group.IsProcessing && time.Since(group.FirstMessageTime) >= time.Second &&
		(len(group.Files) >= 10 || time.Since(group.FirstMessageTime) >= 2*time.Second)
	if shouldProcess {
		group.IsProcessing = true
		go processMediaGroup(bot, msg.MediaGroupID, sheetsSrv, sheetID, driveSrv, parentID, adminID)
	}
	mediaGroupCacheMu.Unlock()
}

func processMediaGroup(bot *tgbotapi.BotAPI, groupID string, sheetsSrv *sheets.Service, sheetID string, driveSrv *drive.Service, parentID string, adminID int64) {
	time.Sleep(500 * time.Millisecond)
	mediaGroupCacheMu.Lock()
	group, exists := mediaGroupCache[groupID]
	if !exists || len(group.Files) == 0 {
		mediaGroupCacheMu.Unlock()
		return
	}
	addr, amt, comm, chatID, username := group.Address, group.Amount, group.Comment, group.ChatID, group.Username
	var photos []*tgbotapi.PhotoSize
	for _, p := range group.Files {
		photos = append(photos, p)
	}
	sort.Slice(photos, func(i, j int) bool {
		return photos[i].Width*photos[i].Height > photos[j].Width*photos[j].Height
	})
	mediaGroupCacheMu.Unlock()

	if addr == "" || amt == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Укажи адрес и сумму в подписи к первому фото группы!"))
		return
	}
	folderID, folderMsg, err := ensureObjectFolder(driveSrv, parentID, addr)
	if err != nil {
		notifyAdminFailure(bot, adminID, fmt.Errorf("ошибка обработки объекта: %v", err), nil)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка обработки объекта: "+err.Error()))
		return
	}
	msk := time.FixedZone("MSK", 3*3600)
	dateFmt := time.Now().In(msk).Format("02.01.2006")
	results := make(chan string, len(photos))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentUploads)
	for i, photo := range photos {
		wg.Add(1)
		go func(i int, p *tgbotapi.PhotoSize) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			fInfo, err := bot.GetFile(tgbotapi.FileConfig{FileID: p.FileID})
			if err != nil {
				return
			}
			fURL := fInfo.Link(bot.Token)
			fName := sanitizeFileName(fmt.Sprintf("%s_%s_%02d_%s.jpg", sanitizeFileName(addr), dateFmt, i+1, amt))
			if link, err := downloadAndUploadFile(fURL, fName, driveSrv, folderID); err == nil {
				results <- link
			}
		}(i, photo)
	}
	wg.Wait()
	close(results)
	var links []string
	for l := range results {
		links = append(links, l)
	}
	if len(links) == 0 {
		notifyAdminFailure(bot, adminID, errors.New("не удалось загрузить фотографии"), nil)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Не удалось загрузить фотографии."))
		return
	}
	parsedData := ParsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  username,
		Date:      time.Now().In(msk).Format("02/01/2006 15:04:05"),
		DriveLink: strings.Join(links, " "),
	}
	if err := appendToSheet(sheetsSrv, sheetID, parsedData); err != nil {
		notifyAdminFailure(bot, adminID, fmt.Errorf("ошибка записи данных в таблицу: %v", err), nil)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка записи данных в таблицу: "+err.Error()))
		return
	}
	mediaGroupCacheMu.Lock()
	delete(mediaGroupCache, groupID)
	mediaGroupCacheMu.Unlock()
	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Чек успешно загружен!\nФото: %d/%d обработано\nАдрес: %s\nСумма: %s\nКомментарий: %s",
		len(links), len(photos), addr, amt, comm)))
	notifyAdminSuccess(bot, adminID, parsedData, nil, folderMsg)
}

// ==========================
// Функция keep-alive для предотвращения засыпания
// ==========================
func keepAlive(url string) {
	// Пинг каждые 5 минут для предотвращения засыпания на бесплатном тарифе Railway
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			if resp, err := http.Get(url); err == nil {
				resp.Body.Close()
			}
		}
	}()
}

// ==========================
// HTTP сервер и обработка обновлений Telegram
// ==========================

func setupHandler(bot *tgbotapi.BotAPI, sheetsSrv *sheets.Service, sheetID string, driveSrv *drive.Service, parentID string, adminID int64) {
	// Очистка устаревших данных медиагрупп
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			mediaGroupCacheMu.Lock()
			now := time.Now()
			for id, data := range mediaGroupCache {
				if now.Sub(data.LastUpdated) > 2*time.Minute {
					delete(mediaGroupCache, id)
				}
			}
			mediaGroupCacheMu.Unlock()
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
			return
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		var update tgbotapi.Update
		if err = json.Unmarshal(body, &update); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		if update.Message != nil {
			// Обработка команды /start или /help
			if update.Message.IsCommand() {
				switch update.Message.Command() {
				case "start", "help":
					sendHelpMessage(bot, update.Message.Chat.ID)
				}
			} else if update.Message.Text == "Начать" {
				// Если пользователь нажал кнопку "Начать"
				sendHelpMessage(bot, update.Message.Chat.ID)
			} else if update.Message.Photo != nil {
				go handleMediaGroupMessage(bot, update.Message, sheetsSrv, sheetID, driveSrv, parentID, adminID)
			}
		}
		w.WriteHeader(http.StatusOK)
	})
}

// sendHelpMessage формирует сообщение с помощью markdown и отправляет его с кнопкой "Начать"
func sendHelpMessage(bot *tgbotapi.BotAPI, chatID int64) {
	var buf bytes.Buffer
	builder := markdown.NewMarkdown(&buf)
	builder.
		H1("Бот для отслеживания чеков!").
		LF().
		PlainText("Бот помогает добавлять информацию о чеках в Google-таблицу и отслеживать расходы.").
		LF().
		PlainText("Отправьте фото чека с подписью в формате:").
		LF().
		CodeBlocks(markdown.SyntaxHighlightGo, "Адрес: ...\nСумма: ...").
		LF().
		PlainText("💡 Подсказка: Введите информацию с клавиатуры.")
	builder.Build()
	helpText := buf.String()

	// Создаем reply-клавиатуру с кнопкой "Начать"
	startButton := tgbotapi.NewKeyboardButton("Начать")
	keyboard := tgbotapi.NewReplyKeyboard(
		[]tgbotapi.KeyboardButton{startButton},
	)

	msg := tgbotapi.NewMessage(chatID, helpText)
	msg.ParseMode = "MarkdownV2"
	msg.ReplyMarkup = keyboard

	if _, err := bot.Send(msg); err != nil {
		log.Println("Ошибка отправки сообщения:", err)
	}
}

// ==========================
// Main функция
// ==========================
func main() {
	telegramToken, sheetID, driveFolderID, adminID, googleClientID, googleClientSecret, webhookURL := loadEnvVars()

	oauthConfig = &oauth2.Config{
		ClientID:     googleClientID,
		ClientSecret: googleClientSecret,
		RedirectURL:  "https://checkstosheets-production.up.railway.app/",
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive.file",
		},
		Endpoint: google.Endpoint,
	}

	// Фоновая задача для автоматического обновления токена каждые 60 минут
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = getOAuthClient(oauthConfig)
		}
	}()

	client, err := getOAuthClient(oauthConfig)
	if err != nil {
		log.Fatalf("OAuth клиент не получен: %v", err)
	}

	sheetsSrv, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Sheets сервис не создан: %v", err)
	}
	driveSrv, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Drive сервис не создан: %v", err)
	}

	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка инициализации бота: %v", err)
	}
	// Отладочный вывод отключён

	parsedURL, err := url.Parse(webhookURL)
	if err != nil {
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}
	webhookCfg := tgbotapi.WebhookConfig{URL: parsedURL, MaxConnections: 40}
	if _, err = bot.Request(webhookCfg); err != nil {
		log.Fatalf("Webhook не установлен: %v", err)
	}

	keepAlive(webhookURL)
	setupHandler(bot, sheetsSrv, sheetID, driveSrv, driveFolderID, adminID)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{Addr: ":" + port}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP сервер не запущен: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
