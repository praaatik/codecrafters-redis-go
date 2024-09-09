package main

import (
	"bufio"
	"encoding/binary"
	"errors"
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

	_, err = server.readRDBFile()
	if err != nil {
		fmt.Println("Unable to read the RDB file")
		//os.Exit(1)
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

func (r *RedisServer) readRDBFile() (map[string]string, error) {
	if len(os.Args) < 4 {
		return nil, errors.New("not enough arguments")
	}

	file, err := os.Open(fmt.Sprintf("%s/%s", os.Args[2], os.Args[4]))

	if err != nil {
		return nil, err
	}
	keys := make(map[string]string)

	//if err != nil {
	//	fmt.Println("error opening file -> ", err.Error())
	//	return nil, err
	//}

	reader := bufio.NewReader(file)
	stat, _ := file.Stat()

	data := make([]byte, stat.Size())

	_, err = reader.Read(data)

	if err != nil {
		fmt.Println("error reading file -> ", err.Error())
		return nil, err
	}

	magicNumber := data[0:5]
	version := data[5:9]

	fmt.Println(string(magicNumber), string(version))

	if string(magicNumber) != "REDIS" {
		return nil, fmt.Errorf("Invalid RDB file format")
	}

	offset := 9 // skipping the header -> "REDIS0011"

	for offset < len(data) {
		switch data[offset] {

		case 0xFA: // auxillary fields
			offset += 1
			_, offset := parseStringEncodedValue(data, offset)
			_, offset = parseStringEncodedValue(data, offset)

		case 0xFE: // database selector
			offset += 1
			parseSizeEncodedValue(data, offset)

		case 0xFF: // end of file
			fmt.Println("End of file.")
			return keys, nil

		default: // other
			valueType := data[offset]
			offset++

			if valueType == 0x00 {
				key, offset := parseStringEncodedValue(data, offset)
				fmt.Println("key -> ", key)

				value, offset := parseStringEncodedValue(data, offset)
				fmt.Println("value -> ", value)

				r.mu.Lock()
				r.store[key] = value
				// if expiryDuration > 0 {
				// 	r.expiry[key] = time.Now().Add(expiryDuration)
				// } else {
				// 	delete(r.expiry, key)
				// }
				r.mu.Unlock()

				if key != "" && value != "" {
					keys[key] = value
				}
			}

		}

	}

	return keys, nil
}

func parseSizeEncodedValue(data []byte, offset int) (int, int) {
	firstByte := data[offset]
	if firstByte>>6 == 0 {
		return int(firstByte), offset + 1
	} else if firstByte>>6 == 1 {
		size := int((firstByte&0x3F)<<8) | int(data[offset+1])
		return size, offset + 2
	} else if firstByte>>6 == 2 {
		size := int(binary.BigEndian.Uint32(data[offset+1 : offset+5]))
		return size, offset + 5
	}
	return 0, offset
}

func parseStringEncodedValue(data []byte, offset int) (string, int) {
	length, offset := parseSizeEncodedValue(data, offset)
	return string(data[offset : offset+length]), offset + length
}
