package internal

import (
	"time"

	"github.com/tidwall/redcon"
	"go.uber.org/zap"
)

type Middleware func(next redcon.Handler) redcon.Handler

type Instrumenter interface {
	Record(cmd string, duration time.Duration)
}

func Prometheus() Middleware {
	return func(next redcon.Handler) redcon.Handler {
		return redcon.HandlerFunc(func(conn redcon.Conn, cmd redcon.Command) {
			// todo: implement me
			next.ServeRESP(conn, cmd)
		})
	}
}

func Logging(logger *zap.Logger) Middleware {
	return func(next redcon.Handler) redcon.Handler {
		return redcon.HandlerFunc(func(conn redcon.Conn, cmd redcon.Command) {
			defer func(t time.Time) {
				logger.Debug("Processed command",
					zap.ByteStrings("cmd", cmd.Args),
					zap.Duration("latency", time.Since(t)),
					zap.String("remoteAddr", conn.RemoteAddr()))
			}(time.Now())
			next.ServeRESP(conn, cmd)
		})
	}
}
