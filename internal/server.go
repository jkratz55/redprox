package internal

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"

	"github.com/jkratz55/redprox/internal/metrics"
)

// todo: implement auth

type Server struct {
	mux                *redcon.ServeMux
	logger             *zap.Logger
	conf               *Config
	clusterStateHolder *clusterStateHolder
	nodes              *clusterNodes
	mutex              sync.Mutex
	readPref           ReadPreference
	metrics            *metrics.Metrics
}

func NewServer(config *Config, logger *zap.Logger) *Server {
	if logger == nil {
		// If a nil Logger is passed use a Nop logger to prevent panics. While
		// non-ideal maybe there are cases where you don't want logging.
		logger = zap.NewNop()
	}
	s := &Server{
		mux:      redcon.NewServeMux(),
		logger:   logger,
		conf:     config,
		mutex:    sync.Mutex{},
		nodes:    newClusterNodes(config),
		readPref: ReadPreference(config.ProxyConfig.ReadPreference),
		metrics:  metrics.NewMetrics(),
	}

	s.clusterStateHolder = newClusterStateHolder(s.refreshClusterState)

	cmdInstrumenter := metrics.NewCommandInstrumenter()

	// Configure routing commands for the commands supported
	s.mux.Handle("ping", Logging(logger)(Prometheus(cmdInstrumenter)(redcon.HandlerFunc(s.ping))))
	s.mux.Handle("get", Logging(logger)(Prometheus(cmdInstrumenter)(redcon.HandlerFunc(s.get))))
	s.mux.Handle("set", Logging(logger)(Prometheus(cmdInstrumenter)(redcon.HandlerFunc(s.set))))
	s.mux.Handle("del", Logging(logger)(Prometheus(cmdInstrumenter)(redcon.HandlerFunc(s.del))))
	s.mux.Handle("mset", Logging(logger)(Prometheus(cmdInstrumenter)(redcon.HandlerFunc(s.mset))))
	s.mux.Handle("mget", Logging(logger)(Prometheus(cmdInstrumenter)(redcon.HandlerFunc(s.mget))))

	return s
}

func (s *Server) ListenAndServe(addr string) error {
	s.logger.Info(fmt.Sprintf("Starting server on port %d", s.conf.ProxyConfig.ServerPort))
	return redcon.ListenAndServe(addr, s.mux.ServeRESP, s.accept, s.close)
}

func (s *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {
	var err error

	tlsConfig := &tls.Config{}
	tlsConfig.Certificates = make([]tls.Certificate, 1)
	tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}

	return redcon.ListenAndServeTLS(addr, s.mux.ServeRESP, s.accept, s.close, tlsConfig)
}

func (s *Server) refreshClusterState(ctx context.Context) (*clusterState, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.logger.Debug("Refreshing cluster state")
	s.metrics.RecordClusterRefresh()

	tempClient := newClient(s.conf.RedisConfig.Addrs[0], s.conf)
	defer tempClient.Close()

	shards, err := tempClient.ClusterShards(ctx).Result()
	if err != nil {
		s.metrics.RecordClusterError()
		s.logger.Error("Error refreshing cluster state: failed to retrieve cluster shards", zap.Error(err))
		return nil, fmt.Errorf("refresh cluster state: %w", err)
	}
	s.logger.Debug("Retrieved cluster state", zap.Any("shards", shards))

	state := newClusterState(s.nodes)

	for i, shard := range shards {
		clusterShard := clusterShard{}
		for _, node := range shard.Nodes {
			addr := fmt.Sprintf("%s:%d", node.IP, node.Port)
			client := s.nodes.GetOrCreate(addr)
			if node.Role == "master" {
				clusterShard.master = client
				state.masters = append(state.masters, addr)
			} else {
				clusterShard.replicas = append(clusterShard.replicas, client)
			}
			clusterNode, ok := s.nodes.nodes[addr]
			if ok {
				clusterNode.SetGeneration(state.generation.Load())
			}
		}
		if len(shard.Slots) != 1 {
			s.logger.Error("Cannot refresh the cluster state: received multiple slot assignments for the same shard")
			return nil, errors.New("refresh cluster state: unexpected slot assignments")
		}
		clusterShard.start = shard.Slots[0].Start
		clusterShard.end = shard.Slots[0].End
		state.shards = append(state.shards, clusterShard)

		// Maps a slot to a shard making it easier to group keys for multi-key operations
		for j := shard.Slots[0].Start; j <= clusterShard.end; j++ {
			state.slotToShard[j] = i
		}
	}

	// Sort shards to ensure we can do binary search on them during lookup
	sort.Slice(state.shards, func(i, j int) bool {
		return state.shards[i].start < state.shards[j].start
	})

	// Cleanup any nodes no longer in use
	time.AfterFunc(time.Minute, func() {
		s.nodes.GC(state.generation.Load())
	})

	return state, nil
}

func (s *Server) ping(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("PONG")
}

func (s *Server) get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		s.logger.Warn("Invalid arguments for GET command",
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	key := string(cmd.Args[1])

	res, err := s.doGet(key)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			s.logger.Debug("Key not found", zap.String("key", key))
			conn.WriteNull()
			return
		} else {
			s.metrics.RecordCommandError("get")
			s.logger.Error("Error proxying command to Redis",
				zap.Error(err),
				zap.String("key", key),
				zap.String("command", string(cmd.Args[0])))
			conn.WriteError(err.Error())
			return
		}
	}

	conn.WriteBulkString(res)
}

func (s *Server) doGet(key string) (string, error) {
	client, err := s.getClient(key, s.readPref)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	res, err := client.Get(ctx, key).Result()
	if err != nil {
		moved, ask, addr := isMovedError(err)
		if moved || ask {
			s.clusterStateHolder.LazyReload()
			client = s.nodes.GetOrCreate(addr)
			return client.Get(context.Background(), key).Result()
		}
		return "", err
	}

	return res, nil
}

func (s *Server) set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		s.logger.Warn("Invalid arguments for SET command",
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	key := string(cmd.Args[1])
	value := string(cmd.Args[2])

	var (
		ttl time.Duration
		nx  bool
		xx  bool
	)

	for i := 3; i < len(cmd.Args); i++ {
		arg := strings.ToUpper(string(cmd.Args[i]))
		switch arg {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "EX":
			if i+1 >= len(cmd.Args) {
				s.logger.Warn("Invalid arguments for SET command",
					zap.ByteStrings("args", cmd.Args))
				conn.WriteError("ERR wrong number of arguments")
				return
			}
			seconds, err := strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil || seconds <= 0 {
				s.logger.Warn("Invalid arguments for SET command",
					zap.ByteStrings("args", cmd.Args))
				conn.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			ttl = time.Duration(seconds) * time.Second
			i++ // Skip the next argument
		case "PX":
			if i+1 >= len(cmd.Args) {
				s.logger.Warn("Invalid arguments for SET command",
					zap.ByteStrings("args", cmd.Args))
				conn.WriteError("ERR wrong number of arguments")
				return
			}
			milliseconds, err := strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil || milliseconds <= 0 {
				s.logger.Warn("Invalid arguments for SET command",
					zap.ByteStrings("args", cmd.Args))
				conn.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			ttl = time.Duration(milliseconds) * time.Millisecond
			i++ // Skip the next argument
		default:
			s.logger.Warn("Invalid arguments for SET command",
				zap.ByteStrings("args", cmd.Args))
			conn.WriteError("ERR wrong number of arguments")
			return
		}
	}

	if nx && xx {
		s.logger.Warn("Invalid arguments for SET command: NX and XX are mutually exclusive",
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError("ERR NX and XX options are mutually exclusive")
		return
	}

	setArgs := &redis.SetArgs{
		Mode: "",
		TTL:  ttl,
	}
	if nx {
		setArgs.Mode = "NX"
	} else if xx {
		setArgs.Mode = "XX"
	}

	res, err := s.doSet(key, value, setArgs)
	if err != nil {
		s.metrics.RecordCommandError("set")
		s.logger.Error("Error proxying command to Redis",
			zap.Error(err),
			zap.ByteStrings("command", cmd.Args))
		conn.WriteError(err.Error())
	} else {
		conn.WriteString(res)
	}
}

func (s *Server) doSet(key string, value string, args *redis.SetArgs) (string, error) {
	client, err := s.getMasterClient(key)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	res, err := client.SetArgs(ctx, key, value, *args).Result()
	if err != nil {
		moved, ask, addr := isMovedError(err)
		if moved || ask {
			s.clusterStateHolder.LazyReload()
			client = s.nodes.GetOrCreate(addr)
			return client.SetArgs(ctx, key, value, *args).Result()
		}
		return "", err
	}

	return res, nil
}

func (s *Server) del(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		s.logger.Warn("Invalid arguments for DEL command",
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	keys := cmd.Args[1:]
	batches, err := s.batchKeys(keys...)
	if err != nil {
		// todo: this technically cannot error
		conn.WriteError(err.Error())
		return
	}

	var (
		totalDeleted atomic.Uint64
		wg           sync.WaitGroup
		lastErr      atomic.Value
	)

	wg.Add(len(batches))
	for _, batch := range batches {
		batchCopy := batch
		go func(keys []string) {
			defer wg.Done()

			client, err := s.getMasterClient(keys[0])
			if err != nil {
				s.metrics.RecordClusterError()
				s.logger.Error("Unable to get handle to master client for slot",
					zap.Error(err))
				lastErr.Store(err)
				return
			}

			deleted, err := client.Del(context.Background(), keys...).Result()
			if err != nil {
				s.metrics.RecordCommandError("del")
				s.logger.Error("Error proxying deletion command to Redis",
					zap.Error(err),
					zap.ByteStrings("args", cmd.Args))
				lastErr.Store(err)
				return
			}

			totalDeleted.Add(uint64(deleted))
		}(batchCopy)
	}

	wg.Wait()
	rawErr := lastErr.Load()
	if rawErr != nil {
		err = rawErr.(error)
		s.logger.Error("Error proxying command to Redis",
			zap.Error(err),
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError(err.Error())
		return
	}

	conn.WriteInt64(int64(totalDeleted.Load()))
}

func (s *Server) mset(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		s.logger.Warn("Invalid arguments for MSET command",
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError("ERR wrong number of arguments")
		return
	}

	if len(cmd.Args)%2 == 0 {
		s.logger.Warn("Invalid arguments for MSET command",
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError("ERR wrong number of arguments: each key must have a value")
		return
	}

	kvPairs := cmd.Args[1:]
	batches := batchKeyValues(kvPairs...)

	var (
		wg      sync.WaitGroup
		lastErr atomic.Value
	)

	wg.Add(len(batches))
	for _, batch := range batches {
		batchCopy := batch
		go func(kvs []keyValue) {
			defer wg.Done()

			client, err := s.getMasterClient(kvs[0].key)
			if err != nil {
				s.metrics.RecordClusterError()
				s.logger.Error("Unable to get handle to master client for slot",
					zap.Error(err))
				lastErr.Store(err)
				return
			}

			msetArgs := flattenKeyValues(kvs)
			_, err = client.MSet(context.Background(), msetArgs...).Result()
			if err != nil {
				s.metrics.RecordCommandError("mset")
				s.logger.Error("Error proxying command to Redis",
					zap.Error(err),
					zap.ByteStrings("args", cmd.Args))
				lastErr.Store(err)
			}
		}(batchCopy)
	}

	wg.Wait()
	rawErr := lastErr.Load()
	if rawErr != nil {
		err := rawErr.(error)
		s.logger.Error("Error proxying command to Redis",
			zap.Error(err),
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError(err.Error())
		return
	}

	conn.WriteBulkString("OK")
}

func (s *Server) mget(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		s.logger.Warn("Invalid arguments for MSET command",
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	keys := cmd.Args[1:]
	batches, err := s.batchKeys(keys...)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	results := make([]interface{}, len(keys))

	// Map each key to its position in the original input slice
	keyIndexMap := make(map[string]int, len(keys))
	for i, key := range keys {
		keyIndexMap[string(key)] = i
	}

	var (
		wg      sync.WaitGroup
		lastErr atomic.Value
	)

	wg.Add(len(batches))
	for _, batch := range batches {
		batchCopy := batch
		go func(keys []string) {
			defer wg.Done()

			client, err := s.getClient(keys[0], s.readPref)
			if err != nil {
				s.metrics.RecordClusterError()
				s.logger.Error("Unable to get handle to master client for slot",
					zap.Error(err))
				lastErr.Store(err)
				return
			}

			res, err := client.MGet(context.Background(), keys...).Result()
			if err != nil {
				s.metrics.RecordCommandError("mget")
				s.logger.Error("Error proxying command to Redis",
					zap.Error(err),
					zap.ByteStrings("args", cmd.Args))
				lastErr.Store(err)
				return
			}

			for i, value := range res {
				key := keys[i]
				index := keyIndexMap[key]
				results[index] = value
			}
		}(batchCopy)
	}

	wg.Wait()
	rawErr := lastErr.Load()
	if rawErr != nil {
		err = rawErr.(error)
		s.logger.Error("Error proxying command to Redis",
			zap.Error(err),
			zap.ByteStrings("args", cmd.Args))
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(results))
	for _, val := range results {
		if val == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(val.(string))
		}
	}
}

func (s *Server) accept(conn redcon.Conn) bool {
	s.metrics.RecordConnAccepted()
	s.logger.Debug(fmt.Sprintf("Incoming connection from %s", conn.RemoteAddr()))
	return true
}

func (s *Server) close(conn redcon.Conn, err error) {
	s.metrics.RecordConnClosed()
	if err != nil {
		s.logger.Error("Connection closed with error",
			zap.Error(err),
			zap.String("remoteAddr", conn.RemoteAddr()))
	} else {
		s.logger.Debug(fmt.Sprintf("Connection closed to remote %s", conn.RemoteAddr()))
	}
}

func (s *Server) getClient(key string, readPref ReadPreference) (*redis.Client, error) {
	slot := int64(Slot(key))

	state, err := s.clusterStateHolder.Get(context.Background())
	if err != nil {
		return nil, errors.New("ERR cluster state unknown")
	}
	return state.ClientForSlot(slot, readPref), nil
}

func (s *Server) getMasterClient(key string) (*redis.Client, error) {
	slot := int64(Slot(key))

	state, err := s.clusterStateHolder.Get(context.Background())
	if err != nil {
		return nil, errors.New("ERR cluster state unknown")
	}
	return state.MasterForSlot(slot), nil
}

func (s *Server) getSlaveClient(key string) (*redis.Client, error) {
	slot := int64(Slot(key))

	state, err := s.clusterStateHolder.Get(context.Background())
	if err != nil {
		return nil, errors.New("ERR cluster state unknown")
	}
	client, ok := state.SlaveForSlot(slot)
	if !ok {
		return nil, errors.New("ERR no replicas available for slot")
	}
	return client, nil
}

func (s *Server) batchKeys(keys ...[]byte) (map[int64][]string, error) {
	batches := make(map[int64][]string)

	for _, key := range keys {
		slot := int64(Slot(string(key)))
		batches[slot] = append(batches[slot], string(key))
	}
	return batches, nil
}

type keyValue struct {
	key   string
	value []byte
}

func batchKeyValues(kvs ...[]byte) map[int64][]keyValue {
	batches := make(map[int64][]keyValue)
	for i := 0; i < len(kvs); i += 2 {
		key := string(kvs[i])
		value := kvs[i+1]

		slot := int64(Slot(key))
		batches[slot] = append(batches[slot], keyValue{key, value})
	}
	return batches
}

func flattenKeyValues(keyValues []keyValue) []interface{} {
	values := make([]interface{}, len(keyValues)*2)
	for i := 0; i < len(keyValues); i += 2 {
		values[i] = keyValues[i].key
		values[i+1] = keyValues[i].value
	}
	return values
}
