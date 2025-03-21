package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"checkbot/api"
	"checkbot/config"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var blockedInteractiveMode = map[int64]bool{
	123456789: true,
}

type SessionData struct {
	Mode           string
	SelectedObject string
	MediaFiles     map[string]*tgbotapi.PhotoSize
	Amount         string
	Comment        string
	LastUpdate     time.Time
	ChatID         int64
	Username       string
}

var sessionStore = struct {
	sync.RWMutex
	data map[int64]*SessionData
}{data: make(map[int64]*SessionData)}

func sendObjectSelection(bot *tgbotapi.BotAPI, chatID int64) {
	if blockedInteractiveMode[chatID] {
		bot.Send(tgbotapi.NewMessage(chatID, "Интерактивный режим для вас недоступен. Попробуйте отправить фото с подписью."))
		return
	}
	var buttons [][]tgbotapi.InlineKeyboardButton
	for _, obj := range config.ObjectsAsList() {
		btn := tgbotapi.NewInlineKeyboardButtonData(obj, "select_obj:"+obj)
		buttons = append(buttons, tgbotapi.NewInlineKeyboardRow(btn))
	}
	msg := tgbotapi.NewMessage(chatID, "Выберите объект:")
	msg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(buttons...)
	bot.Send(msg)
	sessionStore.Lock()
	sessionStore.data[chatID] = &SessionData{
		Mode:       "interactive",
		MediaFiles: make(map[string]*tgbotapi.PhotoSize),
		LastUpdate: time.Now(),
		ChatID:     chatID,
	}
	sessionStore.Unlock()
}

func handleCallbackQuery(bot *tgbotapi.BotAPI, cq *tgbotapi.CallbackQuery) {
	if strings.HasPrefix(cq.Data, "select_obj:") {
		selected := strings.TrimPrefix(cq.Data, "select_obj:")
		chatID := cq.Message.Chat.ID

		// Блокировка для работы с sessionStore
		sessionStore.Lock()
		if sess, exists := sessionStore.data[chatID]; exists {
			sess.SelectedObject = selected
			sess.LastUpdate = time.Now()
			sessionStore.data[chatID] = sess
		} else {
			sessionStore.data[chatID] = &SessionData{
				Mode:           "interactive",
				SelectedObject: selected,
				MediaFiles:     make(map[string]*tgbotapi.PhotoSize),
				LastUpdate:     time.Now(),
				ChatID:         chatID,
			}
		}
		sessionStore.Unlock()

		// Отправляем ответ на callback-запрос
		callback := tgbotapi.NewCallback(cq.ID, fmt.Sprintf("Объект \"%s\" выбран", selected))
		if _, err := bot.Request(callback); err != nil {
			fmt.Printf("Ошибка при ответе на callback: %v\n", err)
			return
		}

		// Отправляем сообщение пользователю
		msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("Объект \"%s\" выбран. Теперь отправьте фото или медиагруппу для этого объекта.", selected))
		if _, err := bot.Send(msg); err != nil {
			fmt.Printf("Ошибка при отправке сообщения: %v\n", err)
		}
	}
}

func handleInteractiveText(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	sessionStore.RLock()
	sess, exists := sessionStore.data[chatID]
	sessionStore.RUnlock()
	if !exists || sess.Mode != "interactive" {
		sendObjectSelection(bot, chatID)
		return
	}
	parts := strings.Split(msg.Text, "\n")
	if len(parts) >= 1 {
		sess.Amount = strings.TrimSpace(parts[0])
		if len(parts) > 1 {
			sess.Comment = strings.Join(parts[1:], "\n")
		}
		sessionStore.Lock()
		sessionStore.data[chatID] = sess
		sessionStore.Unlock()
		processInteractiveSession(bot, sess, msg)
	} else {
		bot.Send(tgbotapi.NewMessage(chatID, "Введите сумму (и опционально комментарий) в виде:\n<сумма>\n<комментарий>"))
	}
}

func handleInteractiveMedia(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	sessionStore.RLock()
	sess, exists := sessionStore.data[chatID]
	sessionStore.RUnlock()
	if !exists || sess.Mode != "interactive" {
		sendObjectSelection(bot, chatID)
		return
	}
	best := msg.Photo[len(msg.Photo)-1]
	sessionStore.Lock()
	sess.MediaFiles[best.FileID] = &best
	sess.LastUpdate = time.Now()
	sessionStore.data[chatID] = sess
	sessionStore.Unlock()
	bot.Send(tgbotapi.NewMessage(chatID, "Фото получено. Теперь отправьте сумму (и комментарий) отдельным сообщением."))
}

func processInteractiveSession(bot *tgbotapi.BotAPI, sess *SessionData, userMsg *tgbotapi.Message) {
	chatID := sess.ChatID
	if sess.SelectedObject == "" || len(sess.MediaFiles) == 0 || sess.Amount == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "Недостаточно данных. Убедитесь, что выбрали объект, загрузили фото и указали сумму."))
		return
	}
	addr := sess.SelectedObject
	amt := sess.Amount
	comm := sess.Comment
	folderID, folderMsg, err := api.EnsureObjectFolder(addr)
	if err != nil {
		api.NotifyAdminFailure(bot, api.AdminID(), fmt.Errorf("ошибка обработки объекта: %v", err), userMsg)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка обработки объекта: "+err.Error()))
		return
	}
	nowStr := time.Now().Format("020106")
	var driveLink string
	for _, photo := range sess.MediaFiles {
		fileURL, err := bot.GetFileDirectURL(photo.FileID)
		if err != nil {
			continue
		}
		fileName := fmt.Sprintf("%s_%s_%s.jpg", api.SanitizeFileName(addr), nowStr, api.SanitizeFileName(amt))
		driveLink, err = api.UploadPhoto(fileURL, folderID, fileName)
		if err == nil {
			break
		}
	}
	if driveLink == "" {
		api.NotifyAdminFailure(bot, api.AdminID(), errors.New("не удалось загрузить фото"), userMsg)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Не удалось загрузить фото."))
		return
	}
	parsedData := api.ParsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  sess.Username,
		Date:      time.Now().Format("02.01.2006 15:04:05"),
		DriveLink: driveLink,
	}
	if err := api.AppendToSheet(parsedData); err != nil {
		api.NotifyAdminFailure(bot, api.AdminID(), fmt.Errorf("ошибка записи в таблицу: %v", err), userMsg)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка записи данных в таблицу: "+err.Error()))
		return
	}
	userMsgText := fmt.Sprintf("✅ Чек успешно загружен!\nОбъект: %s\nСумма: %s\nКомментарий: %s", addr, amt, comm)
	bot.Send(tgbotapi.NewMessage(chatID, userMsgText))
	api.NotifyAdminSuccess(bot, api.AdminID(), parsedData, userMsg, folderMsg)
	sessionStore.Lock()
	delete(sessionStore.data, chatID)
	sessionStore.Unlock()
}

func setupHandler(bot *tgbotapi.BotAPI) {
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
		if update.CallbackQuery != nil {
			go handleCallbackQuery(bot, update.CallbackQuery)
		}
		if update.Message != nil {
			if update.Message.IsCommand() {
				switch update.Message.Command() {
				case "start", "help":
					sendObjectSelection(bot, update.Message.Chat.ID)
				}
			} else {
				if update.Message.Photo != nil {
					go handleInteractiveMedia(bot, update.Message)
				} else if update.Message.Text != "" {
					go handleInteractiveText(bot, update.Message)
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	})
}

func sendHelpMessage(bot *tgbotapi.BotAPI, chatID int64) {
	helpText := "👋 Добро пожаловать!\nВы можете работать двумя способами:\n1. Ручной ввод – отправьте фото с подписью (адрес, сумма, комментарий).\n2. Интерактивный режим – выберите объект из списка, затем загрузите фото и введите сумму.\n\nНажмите /start для начала интерактивного режима."
	msg := tgbotapi.NewMessage(chatID, helpText)
	bot.Send(msg)
}

func keepAlive(url string) {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			if resp, err := http.Get(url); err == nil {
				resp.Body.Close()
			}
		}
	}()
}

func main() {
	telegramToken, _, _, _, webhookURL := api.LoadEnvVars()
	config.LoadObjects()
	bot, err := tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка инициализации бота: %v", err)
	}
	parsedURL, err := url.ParseRequestURI(webhookURL)
	if err != nil {
		log.Fatalf("Неверный формат WEBHOOK_URL: %v", err)
	}
	webhookCfg := tgbotapi.WebhookConfig{URL: parsedURL, MaxConnections: 40}
	if _, err = bot.Request(webhookCfg); err != nil {
		log.Fatalf("Webhook не установлен: %v", err)
	}
	api.Initialize(os.Getenv("GOOGLE_SHEET_ID"), os.Getenv("GOOGLE_DRIVE_FOLDER_ID"))
	keepAlive(webhookURL)
	setupHandler(bot)
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
