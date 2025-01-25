package internal

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type clusterStateHolder struct {
	load      func(ctx context.Context) (*clusterState, error)
	state     atomic.Value
	reloading atomic.Uint32
}

func newClusterStateHolder(fn func(ctx context.Context) (*clusterState, error)) *clusterStateHolder {
	return &clusterStateHolder{
		load: fn,
	}
}

func (c *clusterStateHolder) Get(ctx context.Context) (*clusterState, error) {
	v := c.state.Load()
	if v == nil {
		return c.Reload(ctx)
	}

	state := v.(*clusterState)
	if time.Since(state.createdAt) > 30*time.Second {
		c.LazyReload()
	}
	return state, nil
}

func (c *clusterStateHolder) Reload(ctx context.Context) (*clusterState, error) {
	state, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	c.state.Store(state)
	return state, nil
}

func (c *clusterStateHolder) LazyReload() {
	if !c.reloading.CompareAndSwap(0, 1) {
		return
	}
	go func() {
		defer c.reloading.Store(0)

		_, err := c.Reload(context.Background())
		if err != nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}()
}

type clusterState struct {
	nodes     []string
	masters   []string
	shards    []clusterShard
	createdAt time.Time
}

type clusterShard struct {
	master   *redis.Client
	replicas []*redis.Client
	start    int64
	end      int64
}
