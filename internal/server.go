package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
)

type Server struct {
	mux                *redcon.ServeMux
	logger             *zap.Logger
	conf               *Config
	clusterStateHolder *clusterStateHolder
	mutex              sync.Mutex
}

func NewServer(config *Config, logger *zap.Logger) *Server {
	if logger == nil {
		// If a nil Logger is passed use a Nop logger to prevent panics. While
		// non-ideal maybe there are cases where you don't want logging.
		logger = zap.NewNop()
	}
	s := &Server{
		mux:    redcon.NewServeMux(),
		logger: logger,
		conf:   config,
		mutex:  sync.Mutex{},
	}

	s.clusterStateHolder = newClusterStateHolder(s.refreshClusterState)

	// Initialize Redis clients for each shard in the cluster. If this fails
	// panic as the application cannot function.
	// err := s.refreshClusterState()
	// if err != nil {
	// 	logger.Panic("Failed to initialize cluster state: unable to retrieve target Redis Cluster metadata", zap.Error(err))
	// }

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
	// todo: handle TLS
	return redcon.ListenAndServeTLS(addr, s.mux.ServeRESP, s.accept, s.close, nil)
}

func (s *Server) refreshClusterState(ctx context.Context) (*clusterState, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	tempClient := newClient(s.conf.Addrs[0], s.conf)
	defer tempClient.Close()

	shards, err := tempClient.ClusterShards(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("refresh cluster state: %w", err)
	}

	state := clusterState{}

	for _, shard := range shards {
		clusterShard := clusterShard{}
		for _, node := range shard.Nodes {
			addr := fmt.Sprintf(fmt.Sprintf("%s:%d", node.IP, node.Port))
			state.nodes = append(state.nodes, addr)
			if node.Role == "master" {
				clusterShard.master = newClient(addr, s.conf)
				state.masters = append(state.masters, addr)
			} else {
				clusterShard.replicas = append(clusterShard.replicas, newClient(addr, s.conf))
			}
		}
		if len(shard.Slots) != 1 {
			return nil, errors.New("refresh cluster state: unexpected slot assignments")
		}
		clusterShard.start = shard.Slots[0].Start
		clusterShard.end = shard.Slots[0].End
		state.shards = append(state.shards, clusterShard)
	}

	return &state, nil
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

	slot := int64(Slot(key))

	state, err := s.clusterStateHolder.Get(context.Background())
	if err != nil {
		conn.WriteError("ERR cluster state unknown")
	}

	var client *redis.Client
	for _, shard := range state.shards {
		if slot >= shard.start && slot <= shard.end {
			client = shard.master
		}
	}

	res, err := client.Get(context.Background(), key).Result()
	if err != nil {
		conn.WriteError(err.Error())
	}

	conn.WriteBulkString(res)
}

func (s *Server) set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) >= 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	key := string(cmd.Args[1])

	slot := Slot(key)
	s.logger.Info(fmt.Sprintf("Setting %d to %s", slot, key))

	// todo: implement for real
	conn.WriteInt(1)
}

func (s *Server) del(conn redcon.Conn, cmd redcon.Command) {

}

func (s *Server) mset(conn redcon.Conn, cmd redcon.Command) {

}

func (s *Server) mget(conn redcon.Conn, cmd redcon.Command) {

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
