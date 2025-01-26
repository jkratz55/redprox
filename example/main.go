package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{"192.168.50.160:6379"},
		Password: "limited",
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}

	if err := client.Set(context.Background(), "key", "value", 0).Err(); err != nil {
		panic(err)
	}

	val, err := client.Get(context.Background(), "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	select {}
}
