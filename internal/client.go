package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

func newClient(addr string, conf *Config) *redis.Client {
	// todo: make most if not all the configuration available to be changed
	return redis.NewClient(&redis.Options{
		Addr:                  addr,
		Username:              conf.RedisConfig.Username,
		Password:              conf.RedisConfig.Password,
		MaxRetries:            conf.RedisConfig.MaxRetries,
		DialTimeout:           conf.RedisConfig.DialTimeout,
		ReadTimeout:           conf.RedisConfig.ReadTimeout,
		WriteTimeout:          conf.RedisConfig.WriteTimeout,
		ContextTimeoutEnabled: false,
		PoolSize:              conf.RedisConfig.PoolSize,
		PoolTimeout:           conf.RedisConfig.PoolTimeout,
		MinIdleConns:          conf.RedisConfig.MinIdleConns,
		MaxIdleConns:          conf.RedisConfig.MaxIdleConns,
		MaxActiveConns:        conf.RedisConfig.MaxActiveConns,
	})
}

func newReadOnlyClient(addr string, conf *Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:                  addr,
		Username:              conf.RedisConfig.Username,
		Password:              conf.RedisConfig.Password,
		MaxRetries:            conf.RedisConfig.MaxRetries,
		DialTimeout:           conf.RedisConfig.DialTimeout,
		ReadTimeout:           conf.RedisConfig.ReadTimeout,
		WriteTimeout:          conf.RedisConfig.WriteTimeout,
		ContextTimeoutEnabled: false,
		PoolSize:              conf.RedisConfig.PoolSize,
		PoolTimeout:           conf.RedisConfig.PoolTimeout,
		MinIdleConns:          conf.RedisConfig.MinIdleConns,
		MaxIdleConns:          conf.RedisConfig.MaxIdleConns,
		MaxActiveConns:        conf.RedisConfig.MaxActiveConns,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			return cn.ReadOnly(ctx).Err()
		},
	})
}

func getRole(client *redis.Client) (string, error) {
	ctx := context.Background()

	// Retrieve the `INFO REPLICATION` output
	info, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return "", fmt.Errorf("failed to retrieve INFO REPLICATION: %w", err)
	}

	// Look for the `role` field in the response
	var role string
	lines := strings.Split(info, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "role:") {
			role = strings.TrimSpace(strings.Split(line, ":")[1])
			break
		}
	}

	if role == "" {
		return "", fmt.Errorf("role not found in INFO REPLICATION")
	}

	return role, nil
}
