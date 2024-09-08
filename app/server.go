package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

var _ = net.Listen
var _ = os.Exit

type RESPType int

const (
	SimpleString RESPType = iota
	Array
	BulkString
	Null
)

func main() {
	server := NewRedisServer()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	// goroutine to handle the expiry of keys
	go server.handleExpiry()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connections", err.Error())
			continue
		}

		go server.handleConnection(conn)
	}
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		store:  make(map[string]string),
		expiry: make(map[string]time.Time),
	}
}
