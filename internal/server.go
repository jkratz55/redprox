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
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
)

// todo: implement auth
// todo: implement mode to read from replicas for higher scalability

type Server struct {
	mux                *redcon.ServeMux
	logger             *zap.Logger
	conf               *Config
	clusterStateHolder *clusterStateHolder
	nodes              *clusterNodes
	mutex              sync.Mutex
	readPref           ReadPreference
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
		readPref: Slave, // todo: read from configuration
	}

	s.clusterStateHolder = newClusterStateHolder(s.refreshClusterState)

	// Configure routing commands for the commands supported
	s.mux.Handle("ping", Logging(logger)(redcon.HandlerFunc(s.ping))) // todo: clean up later
	s.mux.HandleFunc("get", s.get)
	s.mux.HandleFunc("set", s.set)
	s.mux.HandleFunc("del", s.del)
	s.mux.HandleFunc("mset", s.mset)
	s.mux.HandleFunc("mget", s.mget)

	return s
}

func (s *Server) ListenAndServe(addr string) error {
	s.logger.Info(fmt.Sprintf("Starting server on port %d", s.conf.ServerPort))
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

	tempClient := newClient(s.conf.Addrs[0], s.conf)
	defer tempClient.Close()

	shards, err := tempClient.ClusterShards(ctx).Result()
	if err != nil {
		s.logger.Error("Error refreshing cluster state: failed to retrieve cluster shards", zap.Error(err))
		return nil, fmt.Errorf("refresh cluster state: %w", err)
	}
	s.logger.Debug("Retrieved cluster state", zap.Any("shards", shards))

	state := newClusterState(s.nodes)

	for _, shard := range shards {
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
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	key := string(cmd.Args[1])

	res, err := s.doGet(key)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			conn.WriteNull()
		} else {
			conn.WriteError(err.Error())
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
				conn.WriteError("ERR wrong number of arguments")
				return
			}
			seconds, err := strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil || seconds <= 0 {
				conn.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			ttl = time.Duration(seconds) * time.Second
			i++ // Skip the next argument
		case "PX":
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR wrong number of arguments")
				return
			}
			milliseconds, err := strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil || milliseconds <= 0 {
				conn.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			ttl = time.Duration(milliseconds) * time.Millisecond
			i++ // Skip the next argument
		default:
			conn.WriteError("ERR wrong number of arguments")
			return
		}
	}

	if nx && xx {
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
	// todo: implement me!
}

func (s *Server) mset(conn redcon.Conn, cmd redcon.Command) {
	// todo: implement me!
}

func (s *Server) mget(conn redcon.Conn, cmd redcon.Command) {
	// todo: implement me!
}

func (s *Server) accept(conn redcon.Conn) bool {
	s.logger.Debug(fmt.Sprintf("Incoming connection from %s", conn.RemoteAddr()))
	return true
}

func (s *Server) close(conn redcon.Conn, err error) {
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

// func (s *Server) batchKeys(key ...string) map
