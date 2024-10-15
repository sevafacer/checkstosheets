package utils

import (
	"regexp"
)

// SanitizeFileName удаляет или заменяет запрещенные символы из имени файла
func SanitizeFileName(name string) string {
	re := regexp.MustCompile(`[<>:"/\\|?*]+`)
	return re.ReplaceAllString(name, "_")
}
