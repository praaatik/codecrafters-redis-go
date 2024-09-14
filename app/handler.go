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

// handleCommand processes incoming Redis commands and dispatches them to the appropriate handler functions.
// It returns the RESP-formatted response for the command.
func (r *RedisServer) handleCommand(object RESP) []byte {
	// check if the command is in a valid format
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

	case "XADD":
		return r.handleXAddCommand(args)

	case "TYPE":
		return r.handleTypeCommand(args)

	default:
		return []byte("-ERR unknown command\r\n")
	}
}

// handleTypeCommand processes the TYPE command to determine the type of a key.
// It returns the type of the key in RESP simple string format.
func (r *RedisServer) handleTypeCommand(args []RESP) []byte {
	// len of args are exactly one
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'type' command\r\n")
	}

	key := args[0].String

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if the key is a stream
	if _, isStream := r.streams[key]; isStream {
		return []byte("+stream\r\n")
	}

	// Check if the key is a string (existing functionality)
	_, isString := r.store[key]
	if isString {
		return []byte("+string\r\n")
	}

	return []byte("+none\r\n")
}

func (r *RedisServer) handleXAddCommand(args []RESP) []byte {
	// Validate the number of arguments (must be at least 4 and even number of key-value pairs)
	if len(args) < 4 || len(args)%2 != 0 {
		return []byte("-ERR wrong number of arguments for 'xadd' command\r\n")
	}

	key := args[0].String
	id := args[1].String

	if strings.Contains(id, "*") {
		id = r.autoGenerateID(key, id)
	} else {
		if err := r.validateEntryID(key, id); err != nil {
			return []byte(err.Error())
		}
	}

	// Parse key-value pairs
	keyValues := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		keyValues[args[i].String] = args[i+1].String
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// initializing the stream maps if they aren't initialized
	if r.streams == nil {
		r.streams = make(map[string][]StreamEntry)
	}

	response, exists := r.streams[key]
	fmt.Println(exists)
	fmt.Println(response)

	// append to the stream
	r.streams[key] = append(r.streams[key], StreamEntry{ID: id, KeyValues: keyValues})

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(id), id))
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

// validateEntryID validates that the provided ID is greater than the last entry's ID in the stream.
// Returns an error if the ID is invalid.
func (r *RedisServer) validateEntryID(streamKey, id string) error {
	// Minimum valid entry ID
	minID := "0-0"

	// Check if the stream exists
	r.mu.RLock()
	entries, exists := r.streams[streamKey]
	r.mu.RUnlock()

	if !exists {
		return nil // If the stream doesn't exist, any valid ID is acceptable
	}

	// Parse the provided ID
	idTime, idSeq, err := parseEntryID(id)
	if err != nil {
		return fmt.Errorf("-ERR The ID specified in XADD is invalid\r\n")
	}

	// Validate against the last entry's ID
	r.mu.RLock()
	lastEntry := entries[len(entries)-1]
	r.mu.RUnlock()

	lastIDTime, lastIDSeq, err := parseEntryID(lastEntry.ID)
	if err != nil {
		return fmt.Errorf("-ERR The last entry ID is invalid\r\n")
	}

	if id == minID {
		return fmt.Errorf("-ERR The ID specified in XADD must be greater than 0-0\r\n")
	}

	if idTime < lastIDTime || (idTime == lastIDTime && idSeq <= lastIDSeq) {
		return fmt.Errorf("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
	}

	return nil
}

// parseEntryID parses an entry ID into its time and sequence number components.
func parseEntryID(id string) (string, int, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid ID format")
	}

	timePart, seqPart := parts[0], parts[1]
	idSeq, err := strconv.Atoi(seqPart)
	if err != nil {
		return "", 0, err
	}

	return timePart, idSeq, nil
}

// autoGenerateID generates a new entry ID with an auto-incremented sequence number.
// It takes the stream key and the ID template (e.g., "5-*") as input.
func (r *RedisServer) autoGenerateID(streamKey, idTemplate string) string {
	// Parse the ID template
	parts := strings.Split(idTemplate, "-")
	if len(parts) != 2 {
		return "ERR Invalid ID format\r\n"
	}

	timePart := parts[0]
	var sequenceNumber int

	// Default sequence number is 0, except when timePart is 0
	if timePart == "0" {
		sequenceNumber = 1
	} else {
		sequenceNumber = 0
	}

	// Check if the stream exists
	r.mu.RLock()
	entries, exists := r.streams[streamKey]
	r.mu.RUnlock()

	if exists {
		// Find the last entry with the same time part
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			entryTimePart, entrySeqPart, _ := parseEntryID(entry.ID)
			if entryTimePart == timePart {
				sequenceNumber = entrySeqPart + 1
				break
			}
		}
	}

	return fmt.Sprintf("%s-%d", timePart, sequenceNumber)
}
