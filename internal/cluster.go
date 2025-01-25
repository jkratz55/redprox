package internal

import (
	"context"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

type clusterStateHolder struct {
	load      func(ctx context.Context) (*clusterState, error)
	state     atomic.Value
	reloading atomic.Uint32
}

type clusterState struct {
	nodes   []string
	masters []string
}

type shard struct {
	master   *redis.Client
	replicas []*redis.Client
}
