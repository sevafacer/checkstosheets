Telegram to Google Sheets & Drive Bot

Table of Contents
Overview
Features
Demo
Installation
Configuration
Usage
Contributing
License
Contact
Overview
Telegram to Google Sheets & Drive Bot is a robust Telegram bot built with Go (Golang) that automates the process of collecting, organizing, and storing user-submitted data. The bot listens for photo messages with specific captions, parses essential information, uploads the photos to Google Drive, and logs the details into Google Sheets. Additionally, it sends real-time notifications to the developer in case of critical issues, ensuring seamless monitoring and maintenance.

Features
Automated Data Parsing: Extracts address, amount, and comments from user-submitted photo captions using flexible keyword mappings.
Google Drive Integration: Uploads received photos to a specified Google Drive folder.
Google Sheets Logging: Records parsed data into Google Sheets for easy tracking and analysis.
OAuth2 Authentication: Securely manages Google API access with token storage to eliminate the need for repeated authorizations.
Developer Notifications: Sends immediate Telegram alerts to the developer when critical errors occur.
Concurrency Control: Handles multiple incoming messages efficiently with controlled goroutine management.
Graceful Shutdown: Ensures all processes complete smoothly upon termination signals.
Demo
Coming soon!

(Include screenshots or a short video demonstrating the bot in action if available.)

Installation
Prerequisites
Go (Golang): Ensure you have Go installed. You can download it from here.
Telegram Bot Token: Create a Telegram bot using @BotFather to obtain a bot token.
Google Cloud Project: Set up a project in the Google Cloud Console with the Google Sheets and Drive APIs enabled.
Railway Account: Deploy the bot using Railway, which simplifies environment management and deployment.
Clone the Repository
bash
Копировать код
git clone https://github.com/yourusername/telegram-google-bot.git
cd telegram-google-bot
Build the Application
bash
Копировать код
go build -o telegram-google-bot ./cmd/bot
Configuration
The bot requires several environment variables to function correctly. These should be set in Railway's Environment Variables section.

Required Environment Variables
Variable Description Example
TELEGRAM_BOT_TOKEN (Required) The token for your Telegram bot obtained from @BotFather. 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11
GOOGLE_SHEET_ID (Required) The ID of the Google Sheet where data will be logged. 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms
GOOGLE_DRIVE_FOLDER_ID (Required) The ID of the Google Drive folder where photos will be uploaded. 0BwwA4oUTeiV1TGRPeTVjaWRDY1E
GOOGLE_OAUTH_CLIENT_ID (Required) Your Google OAuth2 Client ID from the Google Cloud Console. your-google-client-id.apps.googleusercontent.com
GOOGLE_OAUTH_CLIENT_SECRET (Required) Your Google OAuth2 Client Secret from the Google Cloud Console. your-google-client-secret
GOOGLE_OAUTH_TOKEN (Required) Serialized OAuth2 token in JSON format. Obtain this by running the OAuth flow locally and saving the output. {"access_token":"...","refresh_token":"...","expiry":"..."}
WEBHOOK_URL (Required) The publicly accessible URL where Telegram will send updates (e.g., https://yourdomain.com/). https://yourdomain.com/
PORT (Optional) The port on which the HTTP server will listen. Defaults to 8080 if not set. 8080
DEVELOPER_CHAT_ID (Required) Your Telegram Chat ID where you will receive critical error notifications. Obtain your Chat ID using @userinfobot. 123456789
Obtaining the OAuth2 Token
Run the OAuth Flow Locally:

Create a separate Go script (e.g., get_token.go) with the following content to perform the OAuth2 authorization and obtain the token:

go
package main

import (
"context"
"encoding/json"
"fmt"
"log"
"net/http"

    "golang.org/x/oauth2"
    "golang.org/x/oauth2/google"

)

func main() {
config := &oauth2.Config{
ClientID: "your_google_oauth_client_id",
ClientSecret: "your_google_oauth_client_secret",
RedirectURL: "http://localhost:8081/",
Scopes: []string{
"https://www.googleapis.com/auth/spreadsheets",
"https://www.googleapis.com/auth/drive.file",
},
Endpoint: google.Endpoint,
}

    authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
    fmt.Printf("Перейдите по ссылке для авторизации:\n%v\n", authURL)

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
    	Addr:    ":8081",
    	Handler: mux,
    }

    go func() {
    	log.Println("Запуск временного сервера на :8081")
    	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
    		serverErrCh <- err
    	}
    }()

    select {
    case code := <-codeCh:
    	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    	defer cancel()
    	if err := server.Shutdown(ctx); err != nil {
    		log.Printf("Ошибка при завершении работы сервера: %v", err)
    	}
    	tok, err := config.Exchange(context.Background(), code)
    	if err != nil {
    		log.Fatalf("Не удалось обменять код на токен: %v", err)
    	}
    	tokenJSON, err := json.Marshal(tok)
    	if err != nil {
    		log.Fatalf("Не удалось сериализовать токен: %v", err)
    	}
    	fmt.Printf("Ваш OAuth2 токен:\n%s\n", string(tokenJSON))
    case err := <-serverErrCh:
    	log.Fatalf("Сервер завершился с ошибкой: %v", err)
    case <-context.Background().Done():
    	log.Fatalf("Таймаут ожидания OAuth2 кода")
    }

}
Execute the Script:

bash
Копировать код
go run get_token.go
Authorize the Application:

Open the URL printed in the console.
Grant access to your Google account.
After authorization, the token will be displayed in the console.
Set the Token in Railway:

Copy the JSON token.
In Railway, navigate to your project's Environment Variables.
Add GOOGLE_OAUTH_TOKEN with the copied JSON token as its value.
Usage
Once configured and deployed, the bot will automatically handle incoming messages as follows:

Receiving a Photo Message:

A user sends a photo with a caption containing structured information (e.g., address, amount, comment).
Parsing the Caption:

The bot extracts the address, amount, and comment from the caption using predefined keyword mappings.
Uploading to Google Drive:

The photo is uploaded to the specified Google Drive folder.
Logging to Google Sheets:

The parsed data along with metadata (username, date, Drive link) is appended to the designated Google Sheet.
Error Handling:

If any critical error occurs during the process, the bot sends a notification to the developer's Telegram chat.
Example Message Format
makefile
Копировать код
Адрес: ул. Ленина, д. 10
Сумма: 5000 руб.
Комментарий: Оплата аренды за апрель
Attach a photo with the above caption to trigger the bot's processing.

Contributing
Contributions are welcome! Follow these steps to contribute:

Fork the Repository:

Click the Fork button at the top right of this page.

Clone Your Fork:

bash
Копировать код
git clone https://github.com/yourusername/telegram-google-bot.git
cd telegram-google-bot
Create a Branch:

bash
Копировать код
git checkout -b feature/YourFeature
Make Changes:

Implement your feature or bug fix.

Commit Your Changes:

bash
Копировать код
git commit -m "Add Your Feature"
Push to Your Fork:

bash
Копировать код
git push origin feature/YourFeature
Create a Pull Request:

Navigate to the original repository and click New Pull Request.

License
This project is licensed under the MIT License.

Contact
For any questions or feedback, feel free to reach out:

Developer Telegram: @sevafacer
Email: sevafacer@gmail.com
This project is open-source and free to use. Contributions, issues, and feature requests are welcome!

Additional Notes
Replace Placeholders: Ensure you replace all placeholders like yourusername, your_google_client_id, your_google_client_secret, yourtelegramhandle, and your.email@example.com with your actual information.

Security: Never commit your GOOGLE_OAUTH_TOKEN or any sensitive information to the repository. Always use environment variables to manage secrets.

Enhancements: Consider adding badges for build status, coverage, or other relevant metrics if you set up CI/CD pipelines.

Documentation: For more detailed instructions, consider adding additional sections or linking to a docs/ directory if your project grows in complexity.

Testing: Include a section on how to run tests if you add them to your project.

Changelog: Maintain a CHANGELOG.md to document major changes and updates to your project over time.
