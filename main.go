package main

import (
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jkratz55/redprox/internal"
	"github.com/jkratz55/redprox/internal/health"
	"github.com/jkratz55/redprox/internal/log"
)

func main() {

	logger := log.Logger()
	defer logger.Sync()

	config, err := internal.LoadConfig()
	if err != nil {
		logger.Panic("Failed to load config", zap.Error(err))
	}

	// Start health and debug HTTP server
	go func() {
		healthServer := http.Server{
			Addr:    ":6060",
			Handler: health.NewHandler(),
		}
		if err := healthServer.ListenAndServe(); err != nil {
			logger.Panic("Failed to start health server", zap.Error(err))
		}
	}()

	// Start HTTP server for Prometheus metrics
	go func() {
		metricsServer := http.Server{
			Addr:    ":8082",
			Handler: promhttp.Handler(),
		}
		if err := metricsServer.ListenAndServe(); err != nil {
			logger.Error("Failed to start HTTP server for Prometheus metrics", zap.Error(err))
		}
	}()

	server := internal.NewServer(config, logger)

	err = server.ListenAndServe(fmt.Sprintf(":%d", config.ProxyConfig.ServerPort))
	if err != nil {
		logger.Panic("Server unexpectedly terminated", zap.Error(err))
	}
	logger.Info("Goodbye!")
}
