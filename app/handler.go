package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func (r *RedisServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		object, err := parse(reader)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			conn.Write([]byte("-ERR Invalid RESP format\r\n"))
			return
		}

		response := r.handleCommand(object)
		conn.Write(response)
	}
}

func (r *RedisServer) handleCommand(object RESP) []byte {
	if object.Type != Array || len(object.Array) == 0 {
		return []byte("-ERR Invalid command format\r\n")
	}

	cmd := strings.ToUpper(object.Array[0].String)
	args := object.Array[1:]

	switch cmd {
	case "PING":
		return []byte("+PONG\r\n")

	case "ECHO":
		if len(args) != 1 {
			return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
		}
		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[0].String), args[0].String)
		return []byte(response)

	case "SET":
		return r.handleSetCommand(args)

	case "GET":
		return r.handleGetCommand(args)

	default:
		return []byte("-ERR unknown command\r\n")
	}
}
func (r *RedisServer) handleGetCommand(args []RESP) []byte {
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'get' command\r\n")
	}
	key := args[0].String
	r.mu.RLock()
	value, exists := r.store[key]
	r.mu.RUnlock()
	if !exists {
		return []byte("$-1\r\n")
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)

	return []byte(response)
}

func (r *RedisServer) handleSetCommand(args []RESP) []byte {
	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments for 'set' command\r\n")
	}
	key := args[0].String
	value := args[1].String

	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[key] = value
	delete(r.expiry, key)

	if len(args) == 4 {
		option := strings.ToUpper(args[2].String)
		if option == "EX" {
			expirySeconds, err := strconv.Atoi(args[3].String)
			if err != nil {
				return []byte("-ERR invalid expiry time\r\n")
			}
			r.expiry[key] = time.Now().Add(time.Duration(expirySeconds) * time.Second)
		}
	}

	return []byte("+OK\r\n")
}

func (r *RedisServer) handleExpiry() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		for key, expiryTime := range r.expiry {
			if time.Now().After(expiryTime) {
				delete(r.store, key)
				delete(r.expiry, key)
			}
		}
		r.mu.Unlock()
	}

}
