package internal

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/jkratz55/redprox/internal/log"
)

// clusterStateHolder contains the current state of the cluster and handles
// refreshing/reloading the state.
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

// Get retrieves the current cluster state or reloads it if not present. Performs
// lazy reload if the state is older than 30 seconds.
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

// Reload refreshes the cluster state by invoking the load function and updates
// the internal state with the new data.
func (c *clusterStateHolder) Reload(ctx context.Context) (*clusterState, error) {
	state, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	c.state.Store(state)
	return state, nil
}

// LazyReload initiates a non-blocking refresh of the cluster state if not already
// in progress.
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
	nodes      *clusterNodes
	masters    []string
	shards     []clusterShard
	createdAt  time.Time
	generation atomic.Uint64
}

func newClusterState(nodes *clusterNodes) *clusterState {
	c := &clusterState{
		nodes:     nodes,
		masters:   make([]string, 0),
		shards:    make([]clusterShard, 0),
		createdAt: time.Now(),
	}
	c.generation.Store(nodes.NextGeneration())
	return c
}

func (c *clusterState) ClientForSlot(slot int64) *redis.Client {
	idx := sort.Search(len(c.shards), func(i int) bool {
		return c.shards[i].start > slot
	})
	return c.shards[idx-1].master
}

type clusterShard struct {
	master   *redis.Client
	replicas []*redis.Client
	start    int64
	end      int64
	healthy  bool
}

type clusterNodes struct {
	nodes      map[string]*clusterNode
	conf       *Config
	mu         sync.RWMutex
	generation atomic.Uint64
}

func newClusterNodes(conf *Config) *clusterNodes {
	return &clusterNodes{
		nodes: make(map[string]*clusterNode),
		conf:  conf,
	}
}

func (c *clusterNodes) GetOrCreate(addr string) *redis.Client {
	client, ok := c.get(addr)
	if ok {
		return client
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	newClient := newClient(addr, c.conf)
	clusterNode := &clusterNode{
		client:     newClient,
		generation: atomic.Uint64{},
	}
	clusterNode.SetGeneration(c.generation.Load())

	c.nodes[addr] = clusterNode

	return newClient
}

func (c *clusterNodes) GC(generation uint64) {
	nodesToRemove := make([]*clusterNode, 0)
	logger := log.Logger()
	logger.Debug("Garbage collecting cluster nodes no longer in use",
		zap.Uint64("generation", generation))

	c.mu.Lock()

	for addr, node := range c.nodes {
		if node.generation.Load() >= generation {
			logger.Debug(fmt.Sprintf("Skipping node %s due to it being an active node", addr),
				zap.Uint64("generation", generation),
				zap.Uint64("node_generation", node.generation.Load()))
			continue
		}

		logger.Debug(fmt.Sprintf("Deleting node %s", addr),
			zap.Uint64("generation", generation),
			zap.Uint64("node_generation", node.generation.Load()))
		delete(c.nodes, addr)
		nodesToRemove = append(nodesToRemove, node)
	}

	c.mu.Unlock()

	logger.Debug("Cleaning up clients and closing connections to removed nodes")
	for _, node := range nodesToRemove {
		_ = node.client.Close()
	}
}

func (c *clusterNodes) get(addr string) (*redis.Client, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	client, ok := c.nodes[addr]
	if !ok {
		return nil, false
	}
	return client.Client(), ok
}

func (c *clusterNodes) NextGeneration() uint64 {
	return c.generation.Add(1)
}

type clusterNode struct {
	client     *redis.Client
	generation atomic.Uint64
}

func (c *clusterNode) Client() *redis.Client {
	return c.client
}

func (c *clusterNode) SetGeneration(generation uint64) {
	c.generation.Store(generation)
}

func isMovedError(err error) (moved bool, ask bool, addr string) {
	s := err.Error()
	switch {
	case strings.HasPrefix(s, "MOVED "):
		moved = true
	case strings.HasPrefix(s, "ASK "):
		ask = true
	default:
		return
	}

	ind := strings.LastIndex(s, " ")
	if ind == -1 {
		return false, false, ""
	}

	addr = s[ind+1:]
	addr = getAddr(addr)
	return
}

func getAddr(addr string) string {
	ind := strings.LastIndexByte(addr, ':')
	if ind == -1 {
		return ""
	}

	if strings.IndexByte(addr, '.') != -1 {
		return addr
	}

	if addr[0] == '[' {
		return addr
	}
	return net.JoinHostPort(addr[:ind], addr[ind+1:])
}
