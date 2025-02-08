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

// clusterState represents the state of a Redis cluster, including nodes, masters,
// shards, creation time, and generation.
type clusterState struct {
	nodes       *clusterNodes
	masters     []string
	shards      []clusterShard
	slotToShard map[int64]int
	createdAt   time.Time
	generation  atomic.Uint64
	counter     atomic.Uint64
}

func newClusterState(nodes *clusterNodes) *clusterState {
	c := &clusterState{
		nodes:       nodes,
		masters:     make([]string, 0),
		shards:      make([]clusterShard, 0),
		slotToShard: make(map[int64]int),
		createdAt:   time.Now(),
		counter:     atomic.Uint64{},
	}
	c.generation.Store(nodes.NextGeneration())
	return c
}

// ClientForSlot returns the Redis client to be used for the provided slot. The
// pref is used to influence if the operation occurs on the master or a slave
// node. However, it's important to remember write operations MUST be done on
// the master.
func (c *clusterState) ClientForSlot(slot int64, pref ReadPreference) *redis.Client {
	if pref == Master {
		return c.MasterForSlot(slot)
	}

	if pref == Slave {
		client, ok := c.SlaveForSlot(slot)
		if !ok {
			// If there wasn't a replica for the given slot fallback and use the
			// master
			return c.MasterForSlot(slot)
		}
		return client
	}

	// We can only reach this point if someone is abusing/undermining the API
	panic(fmt.Errorf("invalid ReadPreference: %v", pref))
}

func (c *clusterState) MasterForSlot(slot int64) *redis.Client {
	idx := sort.Search(len(c.shards), func(i int) bool {
		return c.shards[i].start > slot
	})
	return c.shards[idx-1].master
}

func (c *clusterState) SlaveForSlot(slot int64) (*redis.Client, bool) {
	idx := sort.Search(len(c.shards), func(i int) bool {
		return c.shards[i].start > slot
	})

	if idx == 0 || idx > len(c.shards) {
		return nil, false
	}

	replicas := c.shards[idx-1].replicas
	if len(replicas) == 0 {
		return nil, false
	}
	i := c.counter.Add(1)
	return replicas[i%uint64(len(replicas))], true
}

// clusterShard represents a shard in a Redis cluster, containing its master client,
// replicas, slot range, and health status.
type clusterShard struct {
	master   *redis.Client
	replicas []*redis.Client
	start    int64
	end      int64
	healthy  bool
}

// clusterNodes represents a collection of cluster nodes, managing their configuration,
// synchronization, and generations.
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
	logger := log.Logger().Named("cluster.nodes")

	client, ok := c.get(addr)
	if ok {
		return client
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	logger.Debug("Initializing new client",
		zap.String("addr", addr))
	newClient := newClient(addr, c.conf)
	logger.Debug("New client initialized",
		zap.String("addr", addr))
	role, err := getRole(newClient)
	if err != nil {
		logger.Error("Unable to determine if client is a master or slave. READONLY mode cannot be enabled if the node is slave",
			zap.String("addr", addr))
	}
	logger.Debug(fmt.Sprintf("Client role is %s", role))
	if role == "slave" {
		_ = newClient.Close()
		newClient = newReadOnlyClient(addr, c.conf)
		logger.Debug("New read-only client initialized",
			zap.String("addr", addr))
	}

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
