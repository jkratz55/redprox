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

type ProxyConfig struct {
	ServerPort     int    `env:"REDPROX_SERVER_PORT,default=6379"`
	ReadPreference string `env:"REDPROX_READ_PREFERENCE,default=1"`
	TlsEnabled     bool   `env:"REDPROX_TLS_ENABLED,default=false"`
	CertFile       string `env:"REDPROX_CERT_FILE"`
	KeyFile        string `env:"REDPROX_KEY_FILE"`
}

type RedisConfig struct {
	Addrs    []string `env:"REDPROX_REDIS_ADDRS,required"`
	Username string   `env:"REDPROX_REDIS_USERNAME"`
	Password string   `env:"REDPROX_REDIS_PASSWORD"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	err := envconfig.Process(context.Background(), &cfg)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &cfg, nil
}
