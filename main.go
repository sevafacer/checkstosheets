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

// Структура для хранения парсенных данных
type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string
}

// Мапа ключевых слов и их синонимов для гибкого парсинга
var fieldKeywords = map[string][]string{
	"address": {"адрес", "объект", "квартира", "школа", "дом", "улица"},
	"amount":  {"сумма", "стоимость", "оплата", "платёж"},
	"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение"},
}

// OAuth2 конфигурация и каналы для обработки авторизации
var (
	oauthConfig *oauth2.Config
	oauthState  = "state-token"
	authCodeCh  = make(chan string)
	wg          sync.WaitGroup
)

// Настройка максимального количества горутин
const maxGoroutines = 10

// Семафор для ограничения числа горутин
var semaphore = make(chan struct{}, maxGoroutines)

// getClient получает OAuth2 клиента
func getClient(config *oauth2.Config) (*http.Client, error) {
	// Запуск HTTP-сервера для получения кода авторизации
	serverErrCh := make(chan error, 1)
	server := startOAuthServer(serverErrCh)

	// Получение ссылки для авторизации
	authURL := config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline)
	fmt.Printf("Перейдите по ссылке для авторизации:\n%v\n", authURL)

	select {
	case code := <-authCodeCh:
		// Обмен кода на токены
		tok, err := config.Exchange(context.Background(), code)
		if err != nil {
			return nil, fmt.Errorf("не удалось обменять код на токен: %v", err)
		}
		// Завершение работы сервера после получения кода
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Ошибка при завершении работы OAuth2 сервера: %v", err)
		}
		return config.Client(context.Background(), tok), nil
	case err := <-serverErrCh:
		return nil, fmt.Errorf("сервер OAuth2 завершился с ошибкой: %v", err)
	}
}

// startOAuthServer запускает локальный HTTP-сервер для обработки редиректа OAuth2
func startOAuthServer(errCh chan<- error) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != oauthState {
			http.Error(w, "Неверный state", http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "Код не найден в запросе", http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, "Авторизация прошла успешно. Вы можете закрыть это окно.")
		authCodeCh <- code
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Запуск сервера в отдельной горутине
	go func() {
		log.Println("Запуск OAuth2 сервера на https://railway.app/")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	return server
}

// getFullName возвращает полное имя пользователя из Telegram
func getFullName(user *tgbotapi.User) string {
	// Проверяем, есть ли LastName
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

// parseMessage извлекает Адрес, Сумму и Комментарий из комментария с использованием мапы ключевых слов
func parseMessage(message string) (address string, amount string, comment string, err error) {
	if message == "" {
		return "", "", "", errors.New("пустое сообщение")
	}

	// Разбиваем сообщение на строки
	lines := strings.Split(message, "\n")
	var addr, amt, comm string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Используем регулярные выражения для более гибкого поиска
		for field, keywords := range fieldKeywords {
			for _, keyword := range keywords {
				// Регулярное выражение для поиска "ключ: значение" с учётом синонимов и регистронезависимости
				pattern := fmt.Sprintf(`(?i)^%s:\s*(.+)`, regexp.QuoteMeta(keyword))
				re := regexp.MustCompile(pattern)
				matches := re.FindStringSubmatch(line)
				if len(matches) == 2 {
					switch field {
					case "address":
						addr = matches[1]
					case "amount":
						amt = matches[1]
					case "comment":
						comm = matches[1]
					}
					break // Если найдено соответствие, не ищем дальше
				}
			}
		}
	}

	// Проверка обязательных полей
	if addr == "" || amt == "" {
		return "", "", "", errors.New("не удалось найти обязательные поля: адрес и сумма")
	}

	// Комментарий может быть пустым
	return addr, amt, comm, nil
}

// sanitizeFileName удаляет или заменяет запрещенные символы из имени файла
func sanitizeFileName(name string) string {
	// Заменяем все символы, кроме букв, цифр и некоторых специальных символов, на подчёркивания
	re := regexp.MustCompile(`[<>:"/\\|?*]+`)
	return re.ReplaceAllString(name, "_")
}

// handlePhotoMessage обрабатывает сообщение с фотографией
func handlePhotoMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, driveFolderId string, adminChatID int64) {
	// Получение комментария из сообщения
	comment := message.Caption

	// Парсинг комментария
	address, amount, commentText, parseErr := parseMessage(comment)
	if parseErr != nil {
		log.Printf("Ошибка при парсинге сообщения: %v", parseErr)
		return
	}

	// Извлечение дополнительных данных
	username := getFullName(message.From)
	dateFormatted := time.Unix(int64(message.Date), 0).Format("02012006_150405")

	// Получение файла из Telegram
	if len(message.Photo) == 0 {
		log.Println("Сообщение не содержит фотографий")
		return
	}

	// Выбираем фото с наибольшим разрешением (последнее в срезе)
	photo := message.Photo[len(message.Photo)-1]
	fileID := photo.FileID
	file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
	if err != nil {
		log.Printf("Ошибка при получении файла: %v", err)
		return
	}

	// Скачивание файла
	fileURL := file.Link(bot.Token)
	resp, err := http.Get(fileURL)
	if err != nil {
		log.Printf("Ошибка при скачивании файла: %v", err)
		return
	}
	defer resp.Body.Close()

	// Создание имени файла: "address_date.jpg"
	sanitizedAddress := sanitizeFileName(address)
	fileName := fmt.Sprintf("%s_%s.jpg", sanitizedAddress, dateFormatted)

	// Сохранение файла во временную директорию
	tmpFile, err := ioutil.TempFile("", fileName)
	if err != nil {
		log.Printf("Не удалось создать временный файл: %v", err)
		return
	}
	defer os.Remove(tmpFile.Name())

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		log.Printf("Ошибка при сохранении файла: %v", err)
		return
	}

	// Загрузка файла на Google Drive
	driveLink, err := uploadFileToDrive(driveService, tmpFile.Name(), fileName, driveFolderId)
	if err != nil {
		log.Printf("Ошибка при загрузке файла на Drive: %v", err)
		return
	}

	// Добавление данных в Google Sheets
	parsedData := ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  username,
		Date:      time.Unix(int64(message.Date), 0).Format("02/01/2006 15:04:05"),
		DriveLink: driveLink,
	}

	err = appendToSheet(sheetsService, spreadsheetId, parsedData)
	if err != nil {
		log.Printf("Ошибка при записи в Google Sheets: %v", err)
		return
	}

	log.Println("Данные успешно добавлены в Google Sheets и файл загружен на Drive")
}

// uploadFileToDrive загружает файл на Google Drive и возвращает ссылку
func uploadFileToDrive(service *drive.Service, filePath, fileName, folderId string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("не удалось открыть файл для загрузки: %v", err)
	}
	defer f.Close()

	file := &drive.File{
		Name:    fileName,
		Parents: []string{folderId},
	}

	res, err := service.Files.Create(file).Media(f).Fields("webViewLink").Do()
	if err != nil {
		return "", fmt.Errorf("не удалось загрузить файл на Google Drive: %v", err)
	}

	return res.WebViewLink, nil
}

// appendToSheet добавляет строку данных в Google Sheets в столбцы B-G
func appendToSheet(service *sheets.Service, spreadsheetId string, data ParsedData) error {
	// Формирование значений для вставки
	values := []interface{}{
		data.Date,      // Столбец B
		data.Username,  // Столбец C
		data.Address,   // Столбец D
		data.Amount,    // Столбец E
		data.Comment,   // Столбец F (может быть пустым)
		data.DriveLink, // Столбец G
	}

	vr := &sheets.ValueRange{
		Values: [][]interface{}{values},
	}

	resp, err := service.Spreadsheets.Values.Get(spreadsheetId, "'Чеки'!B:B").Do()
	if err != nil {
		return fmt.Errorf("не удалось получить данные из Google Sheets: %v", err)
	}

	lastRow := 1
	if len(resp.Values) > 0 {
		lastRow = len(resp.Values) + 1
	}

	rangeStr := fmt.Sprintf("'Чеки'!B%d:G%d", lastRow, lastRow)

	_, err = service.Spreadsheets.Values.Update(spreadsheetId, rangeStr, vr).
		ValueInputOption("RAW").
		Do()
	if err != nil {
		return fmt.Errorf("не удалось обновить Google Sheets: %v", err)
	}

	return nil
}

// notifyAdmin отправляет уведомление администратору бота в Telegram
func notifyAdmin(bot *tgbotapi.BotAPI, adminChatID int64, message string) {
	if adminChatID == 0 {
		return
	}
	msg := tgbotapi.NewMessage(adminChatID, message)
	_, err := bot.Send(msg)
	if err != nil {
		log.Printf("Не удалось отправить уведомление администратору: %v", err)
	}
}

func main() {
	// Чтение переменных окружения
	telegramToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_BOT_TOKEN не установлен в переменных окружения")
	}

	spreadsheetId := os.Getenv("GOOGLE_SHEET_ID")
	if spreadsheetId == "" {
		log.Fatal("GOOGLE_SHEET_ID не установлен в переменных окружения")
	}

	driveFolderId := os.Getenv("GOOGLE_DRIVE_FOLDER_ID")
	if driveFolderId == "" {
		log.Fatal("GOOGLE_DRIVE_FOLDER_ID не установлен в переменных окружения")
	}

	googleClientID := os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret := os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	if googleClientID == "" || googleClientSecret == "" {
		log.Fatal("GOOGLE_OAUTH_CLIENT_ID и GOOGLE_OAUTH_CLIENT_SECRET должны быть установлены в переменных окружения")
	}

	serverAddr := os.Getenv("SERVER_ADDR")
	if serverAddr == "" {
		serverAddr = ":8080"
	}

	adminChatIDStr := os.Getenv("ADMIN_CHAT_ID")
	var adminChatID int64
	if adminChatIDStr != "" {
		var err error
		adminChatID, err = strconv.ParseInt(adminChatIDStr, 10, 64)
		if err != nil {
			log.Fatalf("Не удалось конвертировать ADMIN_CHAT_ID: %v", err)
		}
	}

	// Настройка OAuth2 конфигурации
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

	client, err := getClient(oauthConfig)
	if err != nil {
		log.Fatalf("Не удалось получить OAuth2 клиента: %v", err)
	}

	// Создание Google Sheets сервиса
	sheetsService, err := sheets.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Не удалось создать Sheets сервис: %v", err)
	}

	// Создание Google Drive сервиса
	driveService, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Не удалось создать Drive сервис: %v", err)
	}

	// Создание Telegram бота
	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Panic(err)
	}
	bot.Debug = true
	log.Printf("Авторизовался как %s", bot.Self.UserName)

	// Настройка Webhook
	webhookURL := os.Getenv("WEBHOOK_URL")
	if webhookURL == "" {
		log.Fatal("WEBHOOK_URL не установлен в переменных окружения")
	}

	url, err := url.Parse(webhookURL)
	if err != nil {
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}

	_, err = bot.Request(tgbotapi.WebhookConfig{
		URL: url,
	})
	if err != nil {
		log.Fatalf("Не удалось установить Webhook: %v", err)
	}
	log.Printf("Webhook установлен на %s", webhookURL)

	// Обработчик для входящих обновлений
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			bytes, _ := ioutil.ReadAll(r.Body)
			var update tgbotapi.Update
			err := json.Unmarshal(bytes, &update)
			if err != nil {
				log.Printf("Ошибка декодирования обновления: %v", err)
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}

			if update.Message != nil && update.Message.Photo != nil {
				// Ограничение количества горутин с использованием семафора
				semaphore <- struct{}{} // Блокируем, если достигнуто максимальное количество горутин
				wg.Add(1)
				go func(message *tgbotapi.Message) {
					defer wg.Done()
					handlePhotoMessage(bot, message, sheetsService, spreadsheetId, driveService, driveFolderId, adminChatID)
					<-semaphore // Освобождаем место в канале
				}(update.Message)
			}

			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
		}
	})

	// Получение порта из переменных окружения Railway
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Порт по умолчанию, если не задан в переменных окружения
	}

	// Запуск HTTP-сервера
	log.Printf("Запуск HTTP сервера на порту %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Ошибка запуска HTTP сервера: %v", err)
	}

	// Создание канала для получения сигналов ОС
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Ожидание сигнала завершения (например, Ctrl+C)
	<-quit
	log.Println("Получен сигнал завершения, останавливаем бота...")

	// Удаление Webhook
	_, err = bot.Request(tgbotapi.DeleteWebhookConfig{
		DropPendingUpdates: true,
	})
	if err != nil {
		log.Printf("Не удалось удалить Webhook: %v", err)
	} else {
		log.Println("Webhook удален")
	}

	// Ожидание завершения всех горутин обработки сообщений
	wg.Wait()

	log.Println("Бот успешно остановлен.")
}
