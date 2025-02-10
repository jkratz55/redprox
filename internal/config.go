package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/sethvargo/go-envconfig"
)

type Config struct {
	ProxyConfig ProxyConfig
	RedisConfig RedisConfig
}

type ProxyConfig struct {
	ServerPort     int    `env:"REDPROX_SERVER_PORT,default=6379"`
	ReadPreference int    `env:"REDPROX_READ_PREFERENCE,default=1"`
	TlsEnabled     bool   `env:"REDPROX_TLS_ENABLED,default=false"`
	CertFile       string `env:"REDPROX_CERT_FILE"`
	KeyFile        string `env:"REDPROX_KEY_FILE"`
	LogLevel       string `env:"REDPROX_LOG_LEVEL,default=INFO"`
}

type RedisConfig struct {
	Addrs          []string      `env:"REDPROX_REDIS_ADDRS,required"`
	Username       string        `env:"REDPROX_REDIS_USERNAME"`
	Password       string        `env:"REDPROX_REDIS_PASSWORD"`
	MaxRetries     int           `env:"REDPROX_REDIS_MAX_RETRIES,default=3"`
	DialTimeout    time.Duration `env:"REDPROX_REDIS_DIAL_TIMEOUT,default=2s"`
	ReadTimeout    time.Duration `env:"REDPROX_REDIS_READ_TIMEOUT,default=3s"`
	WriteTimeout   time.Duration `env:"REDPROX_REDIS_WRITE_TIMEOUT,default=3s"`
	PoolTimeout    time.Duration `env:"REDPROX_REDIS_POOL_TIMEOUT,default=2s"`
	PoolSize       int           `env:"REDPROX_REDIS_POOL_SIZE,default=100"`
	MinIdleConns   int           `env:"REDPROX_REDIS_MIN_IDLE_CONNS,default=10"`
	MaxIdleConns   int           `env:"REDPROX_REDIS_MAX_IDLE_CONNS,default=0"`
	MaxActiveConns int           `env:"REDPROX_REDIS_MAX_ACTIVE_CONNS,default=0"`
}

func LoadConfig() (*Config, error) {
	var cfg Config
	err := envconfig.Process(context.Background(), &cfg)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return &cfg, nil
}
