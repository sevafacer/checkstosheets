package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
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

// fieldKeywords мапа ключевых слов и их синонимов для гибкого парсинга
var fieldKeywords = map[string][]string{
	"address": {"адрес", "объект", "квартира", "школа", "дом", "улица"},
	"amount":  {"сумма", "стоимость", "оплата", "платёж"},
	"comment": {"комментарий", "коммент", "прим", "примечание", "дополнение"},
}

// ParseMessage извлекает Address, Amount и Comment из сообщения
func ParseMessage(message string) (address, amount, comment string, err error) {
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

// GetFullName возвращает полное имя пользователя из Telegram
func GetFullName(user *tgbotapi.User) string {
	if user == nil {
		return "Unknown"
	}
	if user.LastName != "" {
		return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
	}
	return user.FirstName
}
