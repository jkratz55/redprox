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

func Prometheus(is Instrumenter) Middleware {
	return func(next redcon.Handler) redcon.Handler {
		return redcon.HandlerFunc(func(conn redcon.Conn, cmd redcon.Command) {
			defer func(ts time.Time) {
				if len(cmd.Args) > 0 {
					cmd := cmd.Args[0]
					is.Record(string(cmd), time.Since(ts))
				}
			}(time.Now())
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
