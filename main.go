package main

import (
	"go.uber.org/zap"

	"github.com/jkratz55/redprox/internal"
)

func main() {

	logger, _ := zap.NewDevelopment()

	server := internal.NewServer(logger)
	server.ListenAndServe(":6379")
	// err := redcon.ListenAndServe(":6379", server, func(conn redcon.Conn) bool {
	// 	return true
	// }, func(conn redcon.Conn, err error) {
	//
	// })
	// if err != nil {
	// 	panic(err)
	// }
}
