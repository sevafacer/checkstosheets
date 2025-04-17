package main

// --- WEBHOOK VERSION for Railway free ---
// Features: Service‑account auth (no manual login), Telegram webhook, HTTP handler, Drive+Sheets uploads.
// Replace all polling code; relies on WEBHOOK_URL env var.

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

// =============================================
// CONFIG & GLOBALS
// =============================================

const (
	mediaGroupTimeout    = 3 * time.Second
	maxConcurrentUploads = 5 // Railway free: limit goroutines
)

var (
	fieldKeywords = map[string][]string{
		"address": {"адрес", "объект", "дом", "квартира", "улица"},
		"amount":  {"сумма", "стоимость", "цена"},
		"comment": {"комментарий", "прим", "заметка"},
	}

	mediaMu sync.Mutex
	groups  = map[string]*mediaGroup{}
)

// =============================================
// TYPES
// =============================================

type parsedData struct {
	Address, Amount, Comment, Username, Date, DriveLink string
}

type mediaGroup struct {
	Files        map[string]*tgbotapi.PhotoSize
	Address      string
	Amount       string
	Comment      string
	Username     string
	ChatID       int64
	ProcessTimer *time.Timer
	LastUpdated  time.Time
	IsProcessing bool
}

// =============================================
// ENV UTILS
// =============================================

func mustEnv(k string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		log.Fatalf("env %s not set", k)
	}
	return v
}

// =============================================
// GOOGLE AUTH & SERVICES (service‑account)
// =============================================

func newGoogleClient(ctx context.Context) *http.Client {
	j := mustEnv("GOOGLE_SERVICE_ACCOUNT_JSON")
	data, err := base64.StdEncoding.DecodeString(j)
	if err != nil {
		log.Fatalf("decode service json: %v", err)
	}
	creds, err := google.CredentialsFromJSON(ctx, data, drive.DriveFileScope, sheets.SpreadsheetsScope)
	if err != nil {
		log.Fatalf("google creds: %v", err)
	}
	return oauth2.NewClient(ctx, creds.TokenSource)
}

// =============================================
// PARSING HELPERS
// =============================================

func cleanAmount(s string) string {
	re := regexp.MustCompile(`[^0-9.,]`)
	return strings.ReplaceAll(re.ReplaceAllString(s, ""), ".", ",")
}

func parseCaption(c string) (addr, amt, comm string, err error) {
	if strings.TrimSpace(c) == "" {
		return "", "", "", errors.New("empty caption")
	}
	norm := strings.Join(strings.Fields(c), " ")
	var matches []struct {
		f    string
		s, e int
	}
	for f, kws := range fieldKeywords {
		for _, kw := range kws {
			re := regexp.MustCompile(fmt.Sprintf(`(?i)%s\s*[:=]`, regexp.QuoteMeta(kw)))
			locs := re.FindAllStringIndex(norm, -1)
			for _, l := range locs {
				matches = append(matches, struct {
					f    string
					s, e int
				}{f, l[0], l[1]})
			}
		}
	}
	if len(matches) == 0 {
		parts := strings.Split(c, "\n")
		if len(parts) < 2 {
			return "", "", "", errors.New("need at least two lines")
		}
		addr = strings.TrimSpace(parts[0])
		amt = cleanAmount(strings.TrimSpace(parts[1]))
		if len(parts) > 2 {
			comm = strings.TrimSpace(strings.Join(parts[2:], "\n"))
		}
		if addr == "" || amt == "" {
			return "", "", "", errors.New("addr/amt missing")
		}
		return
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].s < matches[j].s })
	kv := map[string]string{}
	for i, m := range matches {
		end := len(norm)
		if i < len(matches)-1 {
			end = matches[i+1].s
		}
		kv[m.f] = strings.TrimSpace(norm[m.e:end])
	}
	addr = kv["address"]
	amt = cleanAmount(kv["amount"])
	comm = kv["comment"]
	if addr == "" || amt == "" {
		return "", "", "", errors.New("addr/amt missing")
	}
	return
}

// =============================================
// GOOGLE DRIVE / SHEETS HELPERS
// =============================================

func sanitizeFilename(n string) string {
	re := regexp.MustCompile(`[^а-яА-Яa-zA-Z0-9_\-\s.]`)
	n = re.ReplaceAllString(n, "_")
	return strings.ReplaceAll(strings.TrimSpace(n), " ", "_")
}

func ensureFolder(srv *drive.Service, parent, name string) (string, error) {
	name = sanitizeFilename(name)
	if name == "" {
		name = "Разное"
	}
	q := fmt.Sprintf("name='%s' and '%s' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", name, parent)
	list, err := srv.Files.List().Q(q).Fields("files(id)").Do()
	if err == nil && len(list.Files) > 0 {
		return list.Files[0].Id, nil
	}
	f := &drive.File{Name: name, Parents: []string{parent}, MimeType: "application/vnd.google-apps.folder"}
	nf, err := srv.Files.Create(f).Fields("id").Do()
	if err != nil {
		return "", err
	}
	return nf.Id, nil
}

func uploadPhoto(bot *tgbotapi.BotAPI, fileID, addr, amt string, idx int, drv *drive.Service, folder string) (string, error) {
	url, err := bot.GetFileDirectURL(fileID)
	if err != nil {
		return "", err
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	tmp, _ := os.CreateTemp("", "ph_*.jpg")
	defer os.Remove(tmp.Name())
	io.Copy(tmp, resp.Body)
	tmp.Seek(0, io.SeekStart)
	fname := fmt.Sprintf("%s_%s_%s_%02d.jpg", time.Now().Format("020106"), sanitizeFilename(addr), sanitizeFilename(strings.ReplaceAll(amt, ",", ".")), idx)
	file := &drive.File{Name: fname, Parents: []string{folder}}
	up, err := drv.Files.Create(file).Media(tmp).Fields("webViewLink").Do()
	if err != nil {
		return "", err
	}
	return up.WebViewLink, nil
}

func appendSheet(srv *sheets.Service, sheetID string, d parsedData) error {
	vals := &sheets.ValueRange{Values: [][]interface{}{{d.Date, d.Username, d.Address, d.Amount, d.Comment, d.DriveLink}}}
	_, err := srv.Spreadsheets.Values.Append(sheetID, "'Чеки'!A:F", vals).ValueInputOption("USER_ENTERED").InsertDataOption("INSERT_ROWS").Do()
	return err
}

// =============================================
// TELEGRAM HANDLERS
// =============================================

func handleUpdate(bot *tgbotapi.BotAPI, upd *tgbotapi.Update, drv *drive.Service, folderParent string, sh *sheets.Service, sheetID string, adminID int64) {
	if upd.Message == nil || len(upd.Message.Photo) == 0 {
		return
	}
	m := upd.Message
	// single photo
	if m.MediaGroupID == "" {
		addr, amt, comm, err := parseCaption(m.Caption)
		if err != nil {
			bot.Send(tgbotapi.NewMessage(m.Chat.ID, "❗️ Укажи адрес и сумму в подписи."))
			return
		}
		folder, _ := ensureFolder(drv, folderParent, addr)
		best := m.Photo[len(m.Photo)-1]
		link, err := uploadPhoto(bot, best.FileID, addr, amt, 1, drv, folder)
		if err != nil {
			bot.Send(tgbotapi.NewMessage(m.Chat.ID, "❗️ Не удалось загрузить фото."))
			return
		}
		data := parsedData{Address: addr, Amount: amt, Comment: comm, Username: m.From.FirstName, Date: time.Now().Format("02.01.2006 15:04:05"), DriveLink: link}
		if err := appendSheet(sh, sheetID, data); err != nil {
			bot.Send(tgbotapi.NewMessage(m.Chat.ID, "❗️ Ошибка записи в таблицу."))
			return
		}
		bot.Send(tgbotapi.NewMessage(m.Chat.ID, "✅ Чек загружен!"))
		bot.Send(tgbotapi.NewMessage(adminID, fmt.Sprintf("✅ Новый чек от %s\nАдрес: %s\nСумма: %s", data.Username, addr, amt)))
		return
	}
	// group
	mediaMu.Lock()
	g, ok := groups[m.MediaGroupID]
	if !ok {
		addr, amt, comm, _ := parseCaption(m.Caption)
		g = &mediaGroup{Files: map[string]*tgbotapi.PhotoSize{}, Address: addr, Amount: amt, Comment: comm, Username: m.From.FirstName, ChatID: m.Chat.ID}
		groups[m.MediaGroupID] = g
		g.ProcessTimer = time.AfterFunc(mediaGroupTimeout, func() { processGroup(bot, m.MediaGroupID, drv, folderParent, sh, sheetID, adminID) })
	}
	best := m.Photo[len(m.Photo)-1]
	g.Files[best.FileID] = &best
	if m.Caption != "" && (g.Address == "" || g.Amount == "") {
		addr, amt, comm, _ := parseCaption(m.Caption)
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
	g.LastUpdated = time.Now()
	if g.ProcessTimer != nil {
		g.ProcessTimer.Reset(mediaGroupTimeout)
	}
	mediaMu.Unlock()
}

func processGroup(bot *tgbotapi.BotAPI, gid string, drv *drive.Service, parent string, sh *sheets.Service, sheetID string, adminID int64) {
	mediaMu.Lock()
	g := groups[gid]
	if g == nil || g.IsProcessing {
		mediaMu.Unlock()
		return
	}
	g.IsProcessing = true
	files := g.Files
	addr, amt, comm, chatID, user := g.Address, g.Amount, g.Comment, g.ChatID, g.Username
	mediaMu.Unlock()
	if addr == "" || amt == "" {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Укажи адрес и сумму."))
		return
	}
	folder, _ := ensureFolder(drv, parent, addr)
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
			link, err := uploadPhoto(bot, ph.FileID, addr, amt, i, drv, folder)
			if err == nil {
				linkCh <- link
			} else {
				log.Println("group upload", err)
			}
		}(idx, p)
	}
	wg.Wait()
	close(linkCh)
	links := []string{}
	for l := range linkCh {
		links = append(links, l)
	}
	if len(links) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Не удалось загрузить фото."))
		return
	}
	data := parsedData{Address: addr, Amount: amt, Comment: comm, Username: user, Date: time.Now().Format("02.01.2006 15:04:05"), DriveLink: strings.Join(links, " ")}
	if err := appendSheet(sh, sheetID, data); err != nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❗️ Ошибка таблицы."))
		return
	}
	bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Чек загружен! Фото: %d", len(links))))
	bot.Send(tgbotapi.NewMessage(adminID, fmt.Sprintf("✅ Чек (группа) от %s | %s ₽", user, amt)))
	mediaMu.Lock()
	delete(groups, gid)
	mediaMu.Unlock()
}

// =============================================
// MAIN
// =============================================

func main() {
	botToken := mustEnv("TELEGRAM_BOT_TOKEN")
	webhookURL := mustEnv("WEBHOOK_URL")
	sheetID := mustEnv("GOOGLE_SHEET_ID")
	driveParent := mustEnv("GOOGLE_DRIVE_FOLDER_ID")
	adminID, _ := strconv.ParseInt(mustEnv("ADMIN_CHAT_ID"), 10, 64)

	ctx := context.Background()
	client := newGoogleClient(ctx)
	driveSrv, _ := drive.NewService(ctx, option.WithHTTPClient(client))
	sheetSrv, _ := sheets.NewService(ctx, option.WithHTTPClient(client))

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatalf("bot init: %v", err)
	}

	// Remove old hook & set new
	bot.Request(tgbotapi.DeleteWebhookConfig{DropPendingUpdates: true})
	wh, _ := tgbotapi.NewWebhook(webhookURL)
	if _, err := bot.Request(wh); err != nil {
		log.Fatalf("set webhook: %v", err)
	}
	info, _ := bot.GetWebhookInfo()
	log.Printf("Webhook set to %s, pending %d", info.URL, info.PendingUpdateCount)

	// HTTP handler for Telegram POSTs
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var upd tgbotapi.Update
		if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		go handleUpdate(bot, &upd, driveSrv, driveParent, sheetSrv, sheetID, adminID)
		w.WriteHeader(http.StatusOK)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server listening on :%s (webhook)", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("srv: %v", err)
	}
}
