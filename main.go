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

type ParsedData struct {
	Address   string
	Amount    string
	Comment   string
	Username  string
	Date      string
	DriveLink string
}

var fieldKeywords = map[string][]string{
	"address": {"адрес", "объект", "квартира", "школа", "дом", "улица"},
	"amount":  {"сумма", "стоимость", "оплата", "платёж"},
	"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение"},
}

var (
	oauthConfig *oauth2.Config
	oauthState  = "state-token"
	authCodeCh  = make(chan string)
	wg          sync.WaitGroup
)

const maxGoroutines = 10

var semaphore = make(chan struct{}, maxGoroutines)

func saveTokenToSecrets(token *oauth2.Token) error {
	tokenJSON, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("не удалось сериализовать токен: %v", err)
	}

	err = os.Setenv("GOOGLE_OAUTH_TOKEN", string(tokenJSON))
	if err != nil {
		return fmt.Errorf("не удалось сохранить токен в переменных среды: %v", err)
	}

	return nil
}

func loadTokenFromSecrets() (*oauth2.Token, error) {
	tokenJSON := os.Getenv("GOOGLE_OAUTH_TOKEN")
	if tokenJSON == "" {
		return nil, errors.New("токен не найден в переменных среды")
	}

	var token oauth2.Token
	err := json.Unmarshal([]byte(tokenJSON), &token)
	if err != nil {
		return nil, fmt.Errorf("не удалось десериализовать токен: %v", err)
	}

	return &token, nil
}

func getClient(config *oauth2.Config) (*http.Client, error) {
	token, err := loadTokenFromSecrets()
	if err == nil {
		return config.Client(context.Background(), token), nil
	}

	serverErrCh := make(chan error, 1)
	server := startOAuthServer(serverErrCh)

	authURL := config.AuthCodeURL(oauthState, oauth2.AccessTypeOffline)
	fmt.Printf("Перейдите по ссылке для авторизации:\n%v\n", authURL)

	select {
	case code := <-authCodeCh:
		tok, err := config.Exchange(context.Background(), code)
		if err != nil {
			return nil, fmt.Errorf("не удалось обменять код на токен: %v", err)
		}

		err = saveTokenToSecrets(tok)
		if err != nil {
			return nil, fmt.Errorf("не удалось сохранить токен в секретах: %v", err)
		}

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

	go func() {
		log.Println("Запуск OAuth2 сервера на https://railway.app/")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	return server
}

func getFullName(user *tgbotapi.User) string {
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}

func parseMessage(message string) (address string, amount string, comment string, err error) {
	if message == "" {
		return "", "", "", errors.New("пустое сообщение")
	}

	lines := strings.Split(message, "\n")
	var addr, amt, comm []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		for field, keywords := range fieldKeywords {
			for _, keyword := range keywords {
				pattern := fmt.Sprintf(`(?i)(%s|%s\s*[:=-])\s*(.+)`, regexp.QuoteMeta(keyword), regexp.QuoteMeta(keyword))
				re := regexp.MustCompile(pattern)
				matches := re.FindStringSubmatch(line)
				if len(matches) == 3 {
					value := strings.TrimSpace(matches[2])
					switch field {
					case "address":
						addr = append(addr, value)
					case "amount":
						amt = append(amt, value)
					case "comment":
						comm = append(comm, value)
					}
					break
				}
			}
		}
	}

	if len(addr) == 0 || len(amt) == 0 {
		return "", "", "", errors.New("не удалось найти обязательные поля: адрес и сумма")
	}

	return strings.Join(addr, " "), strings.Join(amt, " "), strings.Join(comm, " "), nil
}

func sanitizeFileName(name string) string {
	re := regexp.MustCompile(`[<>:"/\\|?*]+`)
	return re.ReplaceAllString(name, "_")
}

func sendMessageToAdmin(bot *tgbotapi.BotAPI, adminID int64, message string) {
	msg := tgbotapi.NewMessage(adminID, message)
	_, err := bot.Send(msg)
	if err != nil {
		log.Printf("Ошибка при отправке сообщения админу: %v", err)
	}
}

func handlePhotoMessage(bot *tgbotapi.BotAPI, message *tgbotapi.Message, sheetsService *sheets.Service, spreadsheetId string, driveService *drive.Service, driveFolderId string, adminID int64) {
	comment := message.Caption

	address, amount, commentText, parseErr := parseMessage(comment)
	if parseErr != nil {
		log.Printf("Ошибка при парсинге сообщения: %v", parseErr)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так, проверьте правильность заполнения. Обратите внимание на шаблон сообщения и наличие фото.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при парсинге сообщения: %v", parseErr))
		return
	}

	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		log.Printf("Ошибка при загрузке часового пояса: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Произошла внутренняя ошибка. Пожалуйста, попробуйте позже.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при загрузке часового пояса: %v", err))
		return
	}

	username := getFullName(message.From)
	moscowTime := time.Unix(int64(message.Date), 0).In(loc)
	dateFormatted := moscowTime.Format("02/01/2006 15:04:05")

	if len(message.Photo) == 0 {
		log.Println("Сообщение не содержит фотографий")
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так, проверьте наличие фотографии.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, "Пользователь отправил сообщение без фотографии")
		return
	}

	photo := message.Photo[len(message.Photo)-1]
	fileID := photo.FileID
	file, err := bot.GetFile(tgbotapi.FileConfig{FileID: fileID})
	if err != nil {
		log.Printf("Ошибка при получении файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при загрузке фотографии.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при получении файла: %v", err))
		return
	}

	fileURL := file.Link(bot.Token)
	resp, err := http.Get(fileURL)
	if err != nil {
		log.Printf("Ошибка при скачивании файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при скачивании фотографии.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при скачивании файла: %v", err))
		return
	}
	defer resp.Body.Close()

	sanitizedAddress := sanitizeFileName(address)
	fileName := fmt.Sprintf("%s_%s.jpg", sanitizedAddress, dateFormatted)

	tmpFile, err := ioutil.TempFile("", fileName)
	if err != nil {
		log.Printf("Не удалось создать временный файл: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при сохранении фотографии.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Не удалось создать временный файл: %v", err))
		return
	}
	defer os.Remove(tmpFile.Name())

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		log.Printf("Ошибка при сохранении файла: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при сохранении фотографии.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при сохранении файла: %v", err))
		return
	}

	driveLink, err := uploadFileToDrive(driveService, tmpFile.Name(), fileName, driveFolderId)
	if err != nil {
		log.Printf("Ошибка при загрузке файла на Drive: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при загрузке фотографии на Drive.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при загрузке файла на Google Drive: %v", err))
		return
	}

	parsedData := ParsedData{
		Address:   address,
		Amount:    amount,
		Comment:   commentText,
		Username:  username,
		Date:      dateFormatted,
		DriveLink: driveLink,
	}

	err = appendToSheet(sheetsService, spreadsheetId, parsedData)
	if err != nil {
		log.Printf("Ошибка при записи в Google Sheets: %v", err)
		reply := tgbotapi.NewMessage(message.Chat.ID, "Что-то пошло не так при записи данных в таблицу.")
		bot.Send(reply)
		sendMessageToAdmin(bot, adminID, fmt.Sprintf("Ошибка при записи в Google Sheets: %v", err))
		return
	}

	reply := tgbotapi.NewMessage(message.Chat.ID, "Чек успешно добавлен в таблицу.")
	bot.Send(reply)
	log.Println("Данные успешно добавлены в Google Sheets и файл загружен на Drive")
}

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

func appendToSheet(service *sheets.Service, spreadsheetId string, data ParsedData) error {
	values := []interface{}{
		data.Date,
		data.Username, data.Address,
		data.Amount,
		data.Comment,
		data.DriveLink,
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

	adminIDStr := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
	if adminIDStr == "" {
		log.Fatal("ADMIN_CHAT_ID не установлен в переменных окружения")
	}
	adminID, err := strconv.ParseInt(adminIDStr, 10, 64)
	if err != nil {
		log.Fatalf("Неверный формат ADMIN_CHAT_ID: %v", err)
	}

	googleClientID := os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	googleClientSecret := os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	if googleClientID == "" || googleClientSecret == "" {
		log.Fatal("GOOGLE_OAUTH_CLIENT_ID и GOOGLE_OAUTH_CLIENT_SECRET должны быть установлены в переменных окружения")
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

			if update.Message != nil {
				if update.Message.IsCommand() {
					switch update.Message.Command() {
					case "start", "help":
						helpText := `Привет! Я бот для добавления чеков в таблицу. Вот как меня использовать:

1. Отправьте фотографию чека
2. В подписи к фото укажите:
   - Адрес: [адрес]
   - Сумма: [сумма]
   - Комментарий: [ваш комментарий] (необязательно)

Пример:
Адрес: ул. Пушкина, д. 10
Сумма: 1500 руб
Комментарий: Оплата за сентябрь`

						msg := tgbotapi.NewMessage(update.Message.Chat.ID, helpText)
						bot.Send(msg)
					}
				} else if update.Message.Photo != nil {
					// Ограничение количества горутин с использованием семафора
					semaphore <- struct{}{} // Блокируем, если достигнуто максимальное количество горутин
					wg.Add(1)
					go func(message *tgbotapi.Message) {
						defer wg.Done()
						handlePhotoMessage(bot, message, sheetsService, spreadsheetId, driveService, driveFolderId, adminID)
						<-semaphore // Освобождаем место в канале
					}(update.Message)
				}
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
	server := &http.Server{Addr: ":" + port}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска HTTP сервера: %v", err)
		}
	}()

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

	// Graceful shutdown HTTP-сервера
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Ошибка при остановке HTTP-сервера: %v", err)
	}

	// Ожидание завершения всех горутин обработки сообщений
	wg.Wait()

	log.Println("Бот успешно остановлен.")
}
