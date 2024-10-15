package main

import (
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
	"runtime"
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

// ParsedData структура для хранения парсенных данных
type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string
}

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
	DeveloperChatID    int64  // Chat ID разработчика для уведомлений
	GoogleOAuthToken   string // JSON-сериализованный OAuth2 токен
}

// fieldKeywords мапа ключевых слов и их синонимов для гибкого парсинга
var fieldKeywords = map[string][]string{
	"address": {"адрес", "объект", "квартира", "школа", "дом", "улица"},
	"amount":  {"сумма", "стоимость", "оплата", "платёж"},
	"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение"},
}

// App основная структура приложения
type App struct {
	config        Config
	bot           *tgbotapi.BotAPI
	sheetsService *sheets.Service
	driveService  *drive.Service
	oauthConfig   *oauth2.Config
	semaphore     chan struct{}
	wg            sync.WaitGroup
	oauthToken    *oauth2.Token
}

// NewApp инициализирует новое приложение
func NewApp(cfg Config) (*App, error) {
	app := &App{
		config:    cfg,
		semaphore: make(chan struct{}, 10), // maxGoroutines = 10
	}

	// Настройка OAuth2 конфигурации
	app.oauthConfig = &oauth2.Config{
		ClientID:     cfg.GoogleClientID,
		ClientSecret: cfg.GoogleClientSecret,
		RedirectURL:  "https://checkstosheets-production.up.railway.app/",
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive.file",
		},
		Endpoint: google.Endpoint,
	}

	// Получение OAuth2 токена
	var err error
	app.oauthToken, err = app.getOAuthToken()
	if err != nil {
		app.notifyDeveloper(fmt.Sprintf("Не удалось получить OAuth2 токен: %v", err))
		return nil, fmt.Errorf("не удалось получить OAuth2 токен: %w", err)
	}

	// Создание OAuth2 клиента
	client := app.oauthConfig.Client(context.Background(), app.oauthToken)

	// Создание Google Sheets сервиса
	sheetsService, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		app.notifyDeveloper(fmt.Sprintf("Не удалось создать Sheets сервис: %v", err))
		return nil, fmt.Errorf("не удалось создать Sheets сервис: %w", err)
	}
	app.sheetsService = sheetsService

	// Создание Google Drive сервиса
	driveService, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		app.notifyDeveloper(fmt.Sprintf("Не удалось создать Drive сервис: %v", err))
		return nil, fmt.Errorf("не удалось создать Drive сервис: %w", err)
	}
	app.driveService = driveService

	// Создание Telegram бота
	bot, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		app.notifyDeveloper(fmt.Sprintf("Не удалось создать Telegram бота: %v", err))
		return nil, fmt.Errorf("не удалось создать Telegram бота: %w", err)
	}
	bot.Debug = false // В продакшн режиме лучше отключить отладку
	app.bot = bot

	// Проверка необходимости обновления токена
	if app.oauthToken.Valid() == false {
		log.Println("Токен недействителен, требуется обновление.")
		err := app.refreshOAuthToken()
		if err != nil {
			app.notifyDeveloper(fmt.Sprintf("Не удалось обновить OAuth2 токен: %v", err))
			return nil, fmt.Errorf("не удалось обновить OAuth2 токен: %w", err)
		}
	}

	return app, nil
}

// getOAuthToken получает OAuth2 токен из конфигурации или выполняет поток авторизации
func (app *App) getOAuthToken() (*oauth2.Token, error) {
	if app.config.GoogleOAuthToken != "" {
		var token oauth2.Token
		err := json.Unmarshal([]byte(app.config.GoogleOAuthToken), &token)
		if err != nil {
			return nil, fmt.Errorf("не удалось разобрать GoogleOAuthToken: %w", err)
		}
		return &token, nil
	}

	// Если токен не установлен, выполнить OAuth2 поток
	token, err := app.performOAuthFlow()
	if err != nil {
		return nil, err
	}

	// Вывод токена для сохранения в Railway
	tokenJSON, err := json.Marshal(token)
	if err != nil {
		return nil, fmt.Errorf("не удалось сериализовать токен: %w", err)
	}
	log.Printf("Ваш OAuth2 токен:\n%s\n\nСохраните его в переменной окружения GOOGLE_OAUTH_TOKEN", string(tokenJSON))

	return token, nil
}

// performOAuthFlow выполняет OAuth2 поток для получения токена
func (app *App) performOAuthFlow() (*oauth2.Token, error) {
	authURL := app.oauthConfig.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Перейдите по ссылке для авторизации:\n%v\n", authURL)

	// Запуск временного HTTP-сервера для получения кода авторизации
	codeCh := make(chan string)
	serverErrCh := make(chan error, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != "state-token" {
			http.Error(w, "Неверный state", http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "Код не найден в запросе", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "Авторизация прошла успешно. Вы можете закрыть это окно.")
		codeCh <- code
	})

	server := &http.Server{
		Addr:    ":8081", // Используйте нестандартный порт, чтобы избежать конфликтов
		Handler: mux,
	}

	go func() {
		log.Println("Запуск временного OAuth2 сервера на :8081")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrCh <- err
		}
	}()

	// Ожидание кода авторизации или ошибки
	select {
	case code := <-codeCh:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Ошибка при завершении работы OAuth2 сервера: %v", err)
		}
		tok, err := app.oauthConfig.Exchange(context.Background(), code)
		if err != nil {
			return nil, fmt.Errorf("не удалось обменять код на токен: %w", err)
		}
		return tok, nil
	case err := <-serverErrCh:
		return nil, fmt.Errorf("сервер OAuth2 завершился с ошибкой: %w", err)
	case <-time.After(120 * time.Second):
		return nil, errors.New("таймаут ожидания OAuth2 кода")
	}
}

// refreshOAuthToken обновляет OAuth2 токен, используя refresh токен
func (app *App) refreshOAuthToken() error {
	tokenSource := app.oauthConfig.TokenSource(context.Background(), app.oauthToken)
	newToken, err := tokenSource.Token()
	if err != nil {
		return fmt.Errorf("не удалось обновить токен: %w", err)
	}
	app.oauthToken = newToken
	return nil
}

// getFullName возвращает полное имя пользователя из Telegram
func getFullName(user *tgbotapi.User) string {
	if user == nil {
		return "Unknown"
	}
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

// parseMessage извлекает Address, Amount и Comment из сообщения
func parseMessage(message string) (address, amount, comment string, err error) {
	if strings.TrimSpace(message) == "" {
		return "", "", "", errors.New("пустое сообщение")
	}

	lines := strings.Split(message, "\n")
	fieldsFound := map[string]string{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		for field, keywords := range fieldKeywords {
			for _, keyword := range keywords {
				pattern := fmt.Sprintf(`(?i)^%s:\s*(.+)`, regexp.QuoteMeta(keyword))
				re := regexp.MustCompile(pattern)
				matches := re.FindStringSubmatch(line)
				if len(matches) == 2 {
					// Если поле уже найдено, пропускаем
					if _, exists := fieldsFound[field]; exists {
						continue
					}
					fieldsFound[field] = strings.TrimSpace(matches[1])
					break
				}
			}
		}
	}

	// Проверка обязательных полей
	addr, addrOk := fieldsFound["address"]
	amt, amtOk := fieldsFound["amount"]
	comm, commOk := fieldsFound["comment"]

	if !addrOk || !amtOk {
		return "", "", "", errors.New("не удалось найти обязательные поля: адрес и сумма")
	}

	if commOk {
		return addr, amt, comm, nil
	}

	return addr, amt, "", nil
}

// sanitizeFileName удаляет или заменяет запрещенные символы из имени файла
func sanitizeFileName(name string) string {
	re := regexp.MustCompile(`[<>:"/\\|?*]+`)
	return re.ReplaceAllString(name, "_")
}

// handlePhotoMessage обрабатывает сообщение с фотографией
func (app *App) handlePhotoMessage(ctx context.Context, message *tgbotapi.Message) {
	defer app.wg.Done()
	defer func() { <-app.semaphore }()

	comment := message.Caption

	address, amount, commentText, err := parseMessage(comment)
	if err != nil {
		log.Printf("Ошибка при парсинге сообщения: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Ошибка при парсинге сообщения от пользователя %s: %v", getFullName(message.From), err))
		return
	}

	username := getFullName(message.From)
	dateFormatted := time.Unix(int64(message.Date), 0).Format("02012006_150405")

	if len(message.Photo) == 0 {
		log.Println("Сообщение не содержит фотографий")
		app.notifyDeveloper(fmt.Sprintf("Сообщение от пользователя %s не содержит фотографий", username))
		return
	}

	photo := message.Photo[len(message.Photo)-1]
	fileID := photo.FileID
	fileConfig := tgbotapi.FileConfig{FileID: fileID}
	file, err := app.bot.GetFile(fileConfig)
	if err != nil {
		log.Printf("Ошибка при получении файла: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Ошибка при получении файла от пользователя %s: %v", username, err))
		return
	}

	fileURL := file.Link(app.bot.Token)
	resp, err := http.Get(fileURL)
	if err != nil {
		log.Printf("Ошибка при скачивании файла: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Ошибка при скачивании файла от пользователя %s: %v", username, err))
		return
	}
	defer resp.Body.Close()

	sanitizedAddress := sanitizeFileName(address)
	fileName := fmt.Sprintf("%s_%s.jpg", sanitizedAddress, dateFormatted)

	tmpFile, err := ioutil.TempFile("", fileName)
	if err != nil {
		log.Printf("Не удалось создать временный файл: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Не удалось создать временный файл для пользователя %s: %v", username, err))
		return
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		log.Printf("Ошибка при сохранении файла: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Ошибка при сохранении файла для пользователя %s: %v", username, err))
		return
	}

	driveLink, err := app.uploadFileToDrive(tmpFile.Name(), fileName)
	if err != nil {
		log.Printf("Ошибка при загрузке файла на Drive: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Ошибка при загрузке файла на Drive для пользователя %s: %v", username, err))
		return
	}

	parsedData := ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  username,
		Date:      time.Unix(int64(message.Date), 0).Format("02/01/2006 15:04:05"),
		DriveLink: driveLink,
	}

	if err := app.appendToSheet(parsedData); err != nil {
		log.Printf("Ошибка при записи в Google Sheets: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Ошибка при записи в Google Sheets для пользователя %s: %v", username, err))
		return
	}

	log.Println("Данные успешно добавлены в Google Sheets и файл загружен на Drive")
}

// uploadFileToDrive загружает файл на Google Drive и возвращает ссылку
func (app *App) uploadFileToDrive(filePath, fileName string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("не удалось открыть файл для загрузки: %w", err)
	}
	defer f.Close()

	file := &drive.File{
		Name:    fileName,
		Parents: []string{app.config.DriveFolderID},
	}

	res, err := app.driveService.Files.Create(file).Media(f).Fields("webViewLink").Do()
	if err != nil {
		return "", fmt.Errorf("не удалось загрузить файл на Google Drive: %w", err)
	}

	return res.WebViewLink, nil
}

// appendToSheet добавляет строку данных в Google Sheets в столбцы B-G
func (app *App) appendToSheet(data ParsedData) error {
	values := []interface{}{
		data.Date,      // Столбец B
		data.Username,  // Столбец C
		data.Address,   // Столбец D
		data.Amount,    // Столбец E
		data.Comment,   // Столбец F
		data.DriveLink, // Столбец G
	}

	vr := &sheets.ValueRange{
		Values: [][]interface{}{values},
	}

	// Получение текущего количества строк
	resp, err := app.sheetsService.Spreadsheets.Values.Get(app.config.SpreadsheetID, "'Чеки'!B:B").Do()
	if err != nil {
		return fmt.Errorf("не удалось получить данные из Google Sheets: %w", err)
	}

	lastRow := 1
	if len(resp.Values) > 0 {
		lastRow = len(resp.Values) + 1
	}

	rangeStr := fmt.Sprintf("'Чеки'!B%d:G%d", lastRow, lastRow)

	_, err = app.sheetsService.Spreadsheets.Values.Update(app.config.SpreadsheetID, rangeStr, vr).
		ValueInputOption("RAW").
		Do()
	if err != nil {
		return fmt.Errorf("не удалось обновить Google Sheets: %w", err)
	}

	return nil
}

// handleUpdate обрабатывает входящие обновления от Telegram
func (app *App) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
		return
	}

	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		log.Printf("Ошибка декодирования обновления: %v", err)
		app.notifyDeveloper(fmt.Sprintf("Ошибка декодирования обновления: %v", err))
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if update.Message != nil && len(update.Message.Photo) > 0 {
		app.semaphore <- struct{}{}
		app.wg.Add(1)
		go app.handlePhotoMessage(r.Context(), update.Message)
	}

	w.WriteHeader(http.StatusOK)
}

// Run запускает HTTP сервер и обрабатывает сигналы завершения
func (app *App) Run() error {
	http.HandleFunc("/", app.handleUpdate)

	server := &http.Server{
		Addr:    app.config.ServerAddr,
		Handler: nil,
	}

	// Канал для получения сигналов ОС
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Горутина для запуска сервера
	go func() {
		log.Printf("Запуск HTTP сервера на %s", app.config.ServerAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска HTTP сервера: %v", err)
		}
	}()

	// Блокировка до получения сигнала
	<-quit
	log.Println("Получен сигнал завершения, останавливаем бота...")

	// Контекст для завершения сервера
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Завершение работы сервера
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Ошибка при завершении HTTP сервера: %v", err)
	}

	// Удаление Webhook
	if _, err := app.bot.Request(tgbotapi.DeleteWebhookConfig{DropPendingUpdates: true}); err != nil {
		log.Printf("Не удалось удалить Webhook: %v", err)
	} else {
		log.Println("Webhook удален")
	}

	// Ожидание завершения горутин
	app.wg.Wait()

	log.Println("Бот успешно остановлен.")
	return nil
}

// loadConfig загружает конфигурацию из переменных окружения
func loadConfig() (Config, error) {
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
		var err error
		_, err = fmt.Sscan(developerChatIDStr, &cfg.DeveloperChatID)
		if err != nil {
			return cfg, fmt.Errorf("некорректный формат DEVELOPER_CHAT_ID: %v", err)
		}
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

	if len(missing) > 0 {
		return cfg, fmt.Errorf("отсутствуют обязательные переменные окружения: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

// notifyDeveloper отправляет сообщение разработчику через Telegram
func (app *App) notifyDeveloper(message string) {
	if app.config.DeveloperChatID == 0 {
		log.Println("DEVELOPER_CHAT_ID не задан, уведомление не отправлено")
		return
	}

	msg := tgbotapi.NewMessage(app.config.DeveloperChatID, fmt.Sprintf("⚠️ *Ошибка в боте*\n\n%s", message))
	msg.ParseMode = "Markdown"

	_, err := app.bot.Send(msg)
	if err != nil {
		log.Printf("Не удалось отправить уведомление разработчику: %v", err)
	}
}

// getStackTrace возвращает стек вызовов в строковом виде
func getStackTrace() string {
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

func main() {
	// Настройка логирования
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Загрузка конфигурации
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	// Инициализация приложения
	app, err := NewApp(cfg)
	if err != nil {
		log.Fatalf("Ошибка инициализации приложения: %v", err)
	}

	// Обработка паник и отправка уведомлений
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("Паника: %v\nStack Trace:\n%s", r, getStackTrace())
			log.Println(errMsg)
			app.notifyDeveloper(errMsg)
		}
	}()

	// Настройка Webhook
	webhookURL := cfg.WebhookURL
	urlParsed, err := url.Parse(webhookURL)
	if err != nil {
		app.notifyDeveloper(fmt.Sprintf("Неверный формат WEBHOOK_URL: %v", err))
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}

	_, err = app.bot.Request(tgbotapi.WebhookConfig{
		URL: urlParsed,
	})
	if err != nil {
		app.notifyDeveloper(fmt.Sprintf("Не удалось установить Webhook: %v", err))
		log.Fatalf("Не удалось установить Webhook: %v", err)
	}
	log.Printf("Webhook установлен на %s", webhookURL)

	// Запуск приложения
	if err := app.Run(); err != nil {
		app.notifyDeveloper(fmt.Sprintf("Ошибка выполнения приложения: %v", err))
		log.Fatalf("Ошибка выполнения приложения: %v", err)
	}
}
