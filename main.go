package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// ==============================
// Константы и переменные
// ==============================

const (
	// Ограничения Railway free
	maxConcurrentUploads = 5 // не создаём много горутин
	mediaGroupTimeout    = 3 * time.Second
)

var (
	// ключевые слова для парсинга
	fieldKeywords = map[string][]string{
		"address": {"адрес", "объект", "дом", "квартира", "улица"},
		"amount":  {"сумма", "стоимость", "оплата", "цена"},
		"comment": {"комментарий", "прим", "заметка"},
	}

	// кеш групп фото
	mediaMu sync.Mutex
	groups  = make(map[string]*mediaGroup)
)

// ==============================
// Структуры
// ==============================

type parsedData struct {
	Address, Amount, Comment, Username, Date, DriveLink string
}

type mediaGroup struct {
	Files            map[string]*tgbotapi.PhotoSize
	Address          string
	Amount           string
	Comment          string
	Username         string
	ChatID           int64
	ProcessTimer     *time.Timer
	FirstMessageTime time.Time
	LastUpdated      time.Time
	IsProcessing     bool
}

// ==============================
// Утилиты
// ==============================

func mustEnv(key string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		log.Fatalf("переменная окружения %s не установлена", key)
	}
	return v
}

func cleanAmount(s string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	s = re.ReplaceAllString(s, "")
	return strings.ReplaceAll(s, ".", ",")
}

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[^а-яА-Яa-zA-Z0-9_\-\s.]`)
	cleaned := re.ReplaceAllString(name, "_")
	cleaned = strings.TrimSpace(cleaned)
	cleaned = strings.ReplaceAll(cleaned, " ", "_")
	return cleaned
}

// ==============================
// Google сервисы
// ==============================

func newGoogleClient(ctx context.Context) *http.Client {
	b64 := mustEnv("GOOGLE_SERVICE_ACCOUNT_JSON")
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		log.Fatalf("не удалось декодировать GOOGLE_SERVICE_ACCOUNT_JSON: %v", err)
	}
	cfg, err := google.CredentialsFromJSON(ctx, data, drive.DriveFileScope, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("ошибка создания учётных данных Google: %v", err)
	}
	return oauth2.NewClient(ctx, cfg.TokenSource)
}

// ==============================
// Логика Telegram
// ==============================

func sendHelp(bot *tgbotapi.BotAPI, chatID int64) {
	text := `👋 Я бот для оцифровки чеков.

Отправь фото чека + подпись с:
Адрес: ...
Сумма: ...
Комментарий: ... (необязательно)`
	msg := tgbotapi.NewMessage(chatID, text)
	bot.Send(msg)
}

// ==============================
// Парсинг подписи
// ==============================

func parseMessage(msg string) (addr, amt, comm string, err error) {
	if strings.TrimSpace(msg) == "" {
		return "", "", "", errors.New("пустая подпись")
	}

	normalized := strings.Join(strings.Fields(msg), " ")
	var matches []struct {
		field      string
		start, end int
	}

	for field, kws := range fieldKeywords {
		for _, kw := range kws {
			pattern := fmt.Sprintf(`(?i)%s\s*[:=]`, regexp.QuoteMeta(kw))
			re := regexp.MustCompile(pattern)
			locs := re.FindAllStringIndex(normalized, -1)
			for _, loc := range locs {
				matches = append(matches, struct {
					field      string
					start, end int
				}{field, loc[0], loc[1]})
			}
		}
	}

	if len(matches) == 0 {
		// fallback: addr\namount\ncomment
		parts := strings.Split(msg, "\n")
		if len(parts) < 2 {
			return "", "", "", errors.New("не хватает строк для адреса и суммы")
		}
		addr = strings.TrimSpace(parts[0])
		amt = cleanAmount(strings.TrimSpace(parts[1]))
		if len(parts) > 2 {
			comm = strings.TrimSpace(strings.Join(parts[2:], "\n"))
		}
		if addr == "" || amt == "" {
			return "", "", "", errors.New("адрес или сумма пустые")
		}
		return
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i].start < matches[j].start })
	kv := make(map[string]string)
	for i, m := range matches {
		end := len(normalized)
		if i < len(matches)-1 {
			end = matches[i+1].start
		}
		val := strings.TrimSpace(normalized[m.end:end])
		kv[m.field] = val
	}
	addr = kv["address"]
	amt = cleanAmount(kv["amount"])
	comm = kv["comment"]
	if addr == "" || amt == "" {
		return "", "", "", errors.New("адрес или сумма не найдены")
	}
	return
}

// ==============================
// Работа с Google Drive / Sheets
// ==============================

func ensureFolder(driveSrv *drive.Service, parentID, name string) (string, error) {
	name = sanitizeFilename(name)
	if name == "" {
		name = "Разное"
	}
	query := fmt.Sprintf("name='%s' and '%s' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", name, parentID)
	res, err := driveSrv.Files.List().Q(query).Fields("files(id)").PageSize(1).Do()
	if err == nil && len(res.Files) > 0 {
		return res.Files[0].Id, nil
	}
	folder := &drive.File{Name: name, Parents: []string{parentID}, MimeType: "application/vnd.google-apps.folder"}
	newF, err := driveSrv.Files.Create(folder).Fields("id").Do()
	if err != nil {
		return "", err
	}
	return newF.Id, nil
}

func uploadPhoto(bot *tgbotapi.BotAPI, fileID, addr, amt string, idx int, driveSrv *drive.Service, folderID string) (string, error) {
	url, err := bot.GetFileDirectURL(fileID)
	if err != nil {
		return "", err
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	dateStr := time.Now().Format("020106")
	filename := fmt.Sprintf("%s_%s_%s_%02d.jpg", dateStr, sanitizeFilename(addr), sanitizeFilename(strings.ReplaceAll(amt, ",", ".")), idx)

	tmp, err := os.CreateTemp("", "chk_*.jpg")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		return "", err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	f := &drive.File{Name: filename, Parents: []string{folderID}}
	up, err := driveSrv.Files.Create(f).Media(tmp).Fields("webViewLink").Do()
	if err != nil {
		return "", err
	}
	return up.WebViewLink, nil
}

func appendSheet(srv *sheets.Service, sheetID string, d parsedData) error {
	row := []interface{}{d.Date, d.Username, d.Address, d.Amount, d.Comment, d.DriveLink}
	vr := &sheets.ValueRange{Values: [][]interface{}{row}}
	_, err := srv.Spreadsheets.Values.Append(sheetID, "'Чеки'!A:F", vr).ValueInputOption("USER_ENTERED").InsertDataOption("INSERT_ROWS").Do()
	return err
}

// ==============================
// Обработка сообщений
// ==============================

func processGroup(bot *tgbotapi.BotAPI, gid string, driveSrv *drive.Service, folderParent string, sheetsSrv *sheets.Service, sheetID string, adminID int64) {
	mediaMu.Lock()
	g := groups[gid]
	if g == nil || g.IsProcessing {
		mediaMu.Unlock()
		return
	}
	g.IsProcessing = true
	files := g.Files
	addr, amt, comm := g.Address, g.Amount, g.Comment
	chatID, username := g.ChatID, g.Username
	mediaMu.Unlock()

	if addr == "" || amt == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Не указан адрес или сумма в подписи к группе."))
		return
	}

	folderID, err := ensureFolder(driveSrv, folderParent, addr)
	if err != nil {
		log.Println("folder error:", err)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка создания папки."))
		return
	}

	// Параллельная загрузка с семафором
	sem := make(chan struct{}, maxConcurrentUploads)
	wg := sync.WaitGroup{}
	linkCh := make(chan string, len(files))
	idx := 0
	for _, p := range files {
		idx++
		wg.Add(1)
		go func(i int, ph *tgbotapi.PhotoSize) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			link, err := uploadPhoto(bot, ph.FileID, addr, amt, i, driveSrv, folderID)
			if err == nil {
				linkCh <- link
			} else {
				log.Println("upload error:", err)
			}
		}(idx, p)
	}
	wg.Wait()
	close(linkCh)

	links := make([]string, 0, len(files))
	for l := range linkCh {
		links = append(links, l)
	}
	if len(links) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Не удалось загрузить фото."))
		return
	}

	data := parsedData{
		Address:   addr,
		Amount:    amt,
		Comment:   comm,
		Username:  username,
		Date:      time.Now().Format("02.01.2006 15:04:05"),
		DriveLink: strings.Join(links, " "),
	}
	if err := appendSheet(sheetsSrv, sheetID, data); err != nil {
		log.Println("sheet error:", err)
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка записи в таблицу."))
		return
	}

	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Чек загружен! Фото: %d/%d", len(links), len(files))))
	// уведомление админа
	adminTxt := fmt.Sprintf("✅ Новый чек от %s\nАдрес: %s\nСумма: %s\nКомментарий: %s", username, addr, amt, comm)
	bot.Send(tgbotapi.NewMessage(adminID, adminTxt))

	mediaMu.Lock()
	delete(groups, gid)
	mediaMu.Unlock()
}

// ==============================
// main
// ==============================

func main() {
	// Настройка окружения
	botToken := mustEnv("TELEGRAM_BOT_TOKEN")
	sheetID := mustEnv("GOOGLE_SHEET_ID")
	driveParent := mustEnv("GOOGLE_DRIVE_FOLDER_ID")
	adminStr := mustEnv("ADMIN_CHAT_ID")
	adminID, _ := strconv.ParseInt(adminStr, 10, 64)

	ctx := context.Background()
	client := newGoogleClient(ctx)
	driveSrv, _ := drive.NewService(ctx, option.WithHTTPClient(client))
	sheetsSrv, _ := sheets.NewService(ctx, option.WithHTTPClient(client))

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatalf("bot init error: %v", err)
	}
	log.Printf("Authorized on account %s", bot.Self.UserName)

	// Long-polling вместо webhook — стабильнее на free Railway
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := bot.GetUpdatesChan(u)

	// Очистка неактивных групп
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		for range ticker.C {
			mediaMu.Lock()
			for id, g := range groups {
				if time.Since(g.LastUpdated) > 2*time.Minute {
					delete(groups, id)
				}
			}
			mediaMu.Unlock()
		}
	}()

	for update := range updates {
		if update.Message == nil {
			continue
		}
		m := update.Message
		if m.IsCommand() {
			switch m.Command() {
			case "start", "help":
				sendHelp(bot, m.Chat.ID)
			}
			continue
		}
		if len(m.Photo) == 0 {
			continue
		}

		// ========= группа или одиночное фото =========
		if m.MediaGroupID == "" {
			// одиночное
			addr, amt, comm, err := parseMessage(m.Caption)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(m.Chat.ID, "❗️ Неверная подпись. Укажи адрес и сумму."))
				continue
			}
			folderID, err := ensureFolder(driveSrv, driveParent, addr)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(m.Chat.ID, "❗️ Ошибка папки."))
				continue
			}
			best := m.Photo[len(m.Photo)-1]
			link, err := uploadPhoto(bot, best.FileID, addr, amt, 1, driveSrv, folderID)
			if err != nil {
				bot.Send(tgbotapi.NewMessage(m.Chat.ID, "❗️ Не удалось загрузить фото."))
				continue
			}
			data := parsedData{Address: addr, Amount: amt, Comment: comm, Username: m.From.FirstName, Date: time.Now().Format("02.01.2006 15:04:05"), DriveLink: link}
			if err := appendSheet(sheetsSrv, sheetID, data); err != nil {
				bot.Send(tgbotapi.NewMessage(m.Chat.ID, "❗️ Ошибка таблицы."))
				continue
			}
			bot.Send(tgbotapi.NewMessage(m.Chat.ID, "✅ Чек загружен!"))
			bot.Send(tgbotapi.NewMessage(adminID, fmt.Sprintf("✅ Новый чек от %s\nАдрес: %s\nСумма: %s", data.Username, addr, amt)))
			continue
		}

		// ========= обработка группы =========
		mediaMu.Lock()
		g, ok := groups[m.MediaGroupID]
		if !ok {
			addr, amt, comm, _ := parseMessage(m.Caption)
			g = &mediaGroup{
				Files:            make(map[string]*tgbotapi.PhotoSize),
				Address:          addr,
				Amount:           amt,
				Comment:          comm,
				Username:         m.From.FirstName,
				ChatID:           m.Chat.ID,
				FirstMessageTime: time.Now(),
			}
			groups[m.MediaGroupID] = g
			g.ProcessTimer = time.AfterFunc(mediaGroupTimeout, func() {
				processGroup(bot, m.MediaGroupID, driveSrv, driveParent, sheetsSrv, sheetID, adminID)
			})
		}
		best := m.Photo[len(m.Photo)-1]
		g.Files[best.FileID] = &best
		g.LastUpdated = time.Now()
		if m.Caption != "" && (g.Address == "" || g.Amount == "") {
			addr, amt, comm, _ := parseMessage(m.Caption)
			if addr != "" {
				g.Address = addr
			}
			if amt != "" {
				g.Amount = amt
			}
			if comm != "" {
				g.Comment = comm
			}
		}
		if g.ProcessTimer != nil {
			g.ProcessTimer.Reset(mediaGroupTimeout)
		}
		mediaMu.Unlock()
	}
}
