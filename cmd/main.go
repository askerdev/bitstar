package main

import (
	"log"
	"net/http"

	"github.com/askerdev/bitstar"
	"go.uber.org/zap"
)

func main() {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)

	zapLogger, _ := config.Build()
	defer zapLogger.Sync()

	logger := zapLogger.Sugar()

	s := &http.Server{
		Handler: bitstar.NewHandler(logger),
		Addr:    ":8080",
	}

	logger.Info("listening on :8080")
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
