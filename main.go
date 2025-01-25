package main

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/jkratz55/redprox/internal"
	"github.com/jkratz55/redprox/internal/log"
)

func main() {

	logger := log.Logger()
	defer logger.Sync()

	config, err := internal.LoadConfig()
	if err != nil {
		logger.Panic("Failed to load config", zap.Error(err))
	}

	server := internal.NewServer(config, logger)

	err = server.ListenAndServe(fmt.Sprintf(":%d", config.ServerPort))
	if err != nil {
		logger.Panic("Server unexpectedly terminated", zap.Error(err))
	}
	logger.Info("Goodbye!")
}
