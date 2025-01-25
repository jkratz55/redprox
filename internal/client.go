package internal

import (
	"time"

	"github.com/redis/go-redis/v9"
)

func newClient(addr string, conf *Config) *redis.Client {
	// todo: make most if not all the configuration available to be changed
	return redis.NewClient(&redis.Options{
		Addr:                  addr,
		Username:              conf.Username,
		Password:              conf.Password,
		MaxRetries:            3,
		DialTimeout:           time.Second,
		ReadTimeout:           time.Second * 2,
		WriteTimeout:          time.Second * 3,
		ContextTimeoutEnabled: false,
		PoolSize:              50,
		PoolTimeout:           time.Second,
		MinIdleConns:          10,
		MaxIdleConns:          50,
		MaxActiveConns:        100,
	})
}
