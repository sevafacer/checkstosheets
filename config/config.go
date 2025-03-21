package config

import (
	"log"
	"os"
	"strings"
)

var Objects []string

func LoadObjects() {
	envObjs := os.Getenv("OBJECTS_LIST")
	if envObjs != "" {
		Objects = strings.Split(envObjs, ", ")
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
