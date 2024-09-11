package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
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

	case "TYPE":
		return r.handleTypeCommand(args)

	case "CONFIG":
		if args[0].String != "GET" {
			return []byte("-ERR wrong number of arguments for 'get' command\r\n")
		}

		if os.Args[1] != "--dir" && os.Args[3] != "--dbfilename" {
			return []byte("Unable to access directory/db file")
		}

		if os.Args[2] != "" {
			return r.handleConfigCommand(os.Args[2], "")
		}

		if os.Args[4] != "" {
			return r.handleConfigCommand("", os.Args[4])
		}

		return r.handleConfigCommand(os.Args[2], os.Args[4])
	case "KEYS":
		if args[0].String != "*" {
			return []byte("-ERR wrong number of arguments for 'keys' command\r\n")
		}

		keys, _ := r.readRDBFile()

		var keysArray []string

		for key := range keys {
			keysArray = append(keysArray, key)
		}

		response := ""
		response = response + fmt.Sprintf("*%d\r\n", len(keysArray))

		for _, k := range keysArray {
			response = response + fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
		}

		return []byte(response)

	default:
		return []byte("-ERR unknown command\r\n")
	}
}

func (r *RedisServer) handleTypeCommand(args []RESP) []byte {
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'get' command\r\n")
	}

	key := args[0].String

	_, exists := r.checkKeyExists(key)
	if exists {
		// hard coding the value if the key exists for now
		return []byte("+string\r\n")
	}
	return []byte("+none\r\n")
}

func (r *RedisServer) handleGetCommand(args []RESP) []byte {
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'get' command\r\n")
	}

	key := args[0].String

	r.mu.RLock()
	defer r.mu.RUnlock()

	if expiryTime, exists := r.expiry[key]; exists && time.Now().After(expiryTime) {
		r.mu.RUnlock()
		r.mu.Lock()

		delete(r.store, key)
		delete(r.expiry, key)

		r.mu.Unlock()
		r.mu.RLock()

		return []byte("$-1\r\n")
	}

	value, keyExists := r.checkKeyExists(key)
	if keyExists {
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
	}

	return []byte("$-1\r\n")
}

func (r *RedisServer) checkKeyExists(key string) (string, bool) {
	value, exists := r.store[key]
	return value, exists
}

func (r *RedisServer) handleSetCommand(args []RESP) []byte {
	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments for 'set' command\r\n")
	}

	key := args[0].String
	value := args[1].String

	expiryDuration := time.Duration(0)

	if len(args) > 2 {
		option := strings.ToUpper(args[2].String)
		if option == "PX" && len(args) > 3 {
			ms, err := strconv.Atoi(args[3].String)
			if err != nil {
				return []byte("-ERR PX value is not a valid integer\r\n")
			}
			expiryDuration = time.Duration(ms) * time.Millisecond
		}
	}

	r.mu.Lock()
	r.store[key] = value
	if expiryDuration > 0 {
		r.expiry[key] = time.Now().Add(expiryDuration)
	} else {
		delete(r.expiry, key)
	}
	r.mu.Unlock()

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

func (r *RedisServer) handleConfigCommand(dir, dbFilename string) []byte {
	response := ""

	if dir != "" {
		response = fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(dir), dir)
		return []byte(response)
	}

	if dbFilename != "" {
		response = fmt.Sprintf("*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(dbFilename), dbFilename)
		return []byte(response)
	}

	return []byte(response)
}
