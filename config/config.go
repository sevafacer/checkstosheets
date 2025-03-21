package config

import (
	"encoding/json"
	"log"
	"os"
)

var Objects []string

func LoadObjects() {
	envObjs := os.Getenv("OBJECTS_LIST")
	if envObjs != "" {
		if err := json.Unmarshal([]byte(envObjs), &Objects); err != nil {
			log.Fatalf("Ошибка парсинга объектов из переменной окружения: %v", err)
		}
		return
	}
	log.Fatal("Переменная окружения OBJECTS_LIST не задана")
}

func ObjectsAsList() []string {
	if len(Objects) == 0 {
		LoadObjects()
	}
	return Objects
}
