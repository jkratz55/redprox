package internal

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type Config struct {
	ServerPort int      `env:"REDPROX_SERVER_PORT,default=6379"`
	Addrs      []string `env:"REDPROX_ADDRS,required"`
	Username   string   `env:"REDPROX_USERNAME"`
	Password   string   `env:"REDPROX_PASSWORD"`
	CertFile   string   `env:"REDPROX_CERT"`
	KeyFile    string   `env:"REDPROX_KEY"`
	LogLevel   string   `env:"REDPROX_LOG_LEVEL,default=INFO"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	err := envconfig.Process(context.Background(), &cfg)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &cfg, nil
}
