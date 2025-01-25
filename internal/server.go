package internal

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
)

type Server struct {
	mux            *redcon.ServeMux
	logger         *zap.Logger
	conf           *Config
	slotToMaster   map[int]*redis.Client
	slotToReplicas map[int][]*redis.Client
	mutex          sync.Mutex
}

func NewServer(logger *zap.Logger) *Server {
	if logger == nil {
		// If a nil Logger is passed use a Nop logger to prevent panics. While
		// non-ideal maybe there are cases where you don't want logging.
		logger = zap.NewNop()
	}
	s := &Server{
		mux:    redcon.NewServeMux(),
		logger: logger,
		conf: &Config{ // todo: use config instead of hard coding
			ServerPort: 6379,
			Addrs:      []string{"192.168.50.160:6379"},
			Username:   "",
			Password:   "limited",
			CertFile:   "",
			KeyFile:    "",
			LogLevel:   "DEBUG",
		},
		slotToMaster:   make(map[int]*redis.Client),
		slotToReplicas: make(map[int][]*redis.Client),
		mutex:          sync.Mutex{},
	}

	// Initialize Redis clients for each shard in the cluster. If this fails
	// panic as the application cannot function.
	err := s.init()
	if err != nil {
		panic(fmt.Errorf("failed to initialize server: failed retrieving cluster metadata: %w", err))
	}

	// Configure routing commands for the commands supported
	s.mux.HandleFunc("ping", s.ping)
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
	return redcon.ListenAndServeTLS(addr, s.mux.ServeRESP, func(conn redcon.Conn) bool {
		return true
	}, func(conn redcon.Conn, err error) {

	}, nil)
}

func (s *Server) init() error {
	client := redis.NewClient(&redis.Options{
		Addr:     s.conf.Addrs[0],
		Password: s.conf.Password,
	})

	shards, err := client.ClusterShards(context.Background()).Result()
	if err != nil {
		return err
	}
	fmt.Println("shards:", shards)

	slot, err := client.ClusterKeySlot(context.Background(), "hello").Result()
	if err != nil {
		return err
	}
	fmt.Println("slot:", slot)

	slots, err := client.ClusterSlots(context.Background()).Result()
	if err != nil {
		return err
	}
	fmt.Println("slots:", slots)

	for _, slotRange := range slots {
		masterNode := slotRange.Nodes[0]
		masterAddr := masterNode.Addr

		if _, exists := s.slotToMaster[slotRange.Start]; !exists {
			masterClient := redis.NewClient(&redis.Options{
				Addr:     masterAddr,
				Password: s.conf.Password,
			})
			for slot := slotRange.Start; slot <= slotRange.End; slot++ {
				s.slotToMaster[slot] = masterClient
			}
		}
	}

	return nil
}

// func (s *Server) refreshClusterState

func (s *Server) ping(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("PONG")
}

func (s *Server) get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}
	key := string(cmd.Args[1])

	slot := Slot(key)
	client := s.slotToMaster[slot]

	res, err := client.Get(context.Background(), key).Result()
	if err != nil {
		conn.WriteError(err.Error())
	}

	conn.WriteBulkString(res)
}

func (s *Server) set(conn redcon.Conn, cmd redcon.Command) {

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
