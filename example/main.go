package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {

	client := redis.NewClient(&redis.Options{
		Addr:                  "localhost:6379",
		DialTimeout:           2 * time.Second,
		ReadTimeout:           2 * time.Second,
		WriteTimeout:          2 * time.Second,
		ContextTimeoutEnabled: true,
		PoolFIFO:              false,
		PoolSize:              50,
		PoolTimeout:           1 * time.Second,
		MinIdleConns:          10,
		MaxIdleConns:          20,
	})
	defer client.Close()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := client.Ping(ctx).Result()
		if err != nil {
			panic(err)
		}
	}()

	keys := make([]string, 0)
	for i := 0; i < 10000; i++ {
		key := uuid.New().String()
		keys = append(keys, key)
		_, err := client.Set(context.Background(), key, "hello", 0).Result()
		if err != nil {
			fmt.Println(err)
		}
	}

	for _, key := range keys {
		val, err := client.Get(context.Background(), key).Result()
		if err != nil {
			fmt.Println(err)
			continue
		}
		if val != "hello" {
			fmt.Println("Got wrong value for key:", key)
		}
	}

	for i := 0; i < 1000; i++ {
		res, err := client.MGet(context.Background(), keys...).Result()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println(res)
	}
}
