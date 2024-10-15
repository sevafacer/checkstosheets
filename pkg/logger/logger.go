package logger

import (
	"log"
	"os"
)

// Logger структура логгера
type Logger struct {
	*log.Logger
}

// NewLogger создает новый логгер
func NewLogger() *Logger {
	return &Logger{
		Logger: log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile),
	}
}

// Fatalf логирует сообщение и завершает работу
func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.Printf(format, v...)
	os.Exit(1)
}
