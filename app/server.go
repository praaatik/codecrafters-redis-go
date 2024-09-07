package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("error accepting connections", err.Error())
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		object, err := parse(reader)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			conn.Write([]byte("Invalid RESP format\r\n"))
			return
		}

		response := handleCommand(object)
		conn.Write(response)
	}
}

type RESP struct {
	Type   RESPType
	String string
	Int    int
	Array  []RESP
}

func parse(reader *bufio.Reader) (RESP, error) {
	p, err := reader.ReadByte()
	if err != nil {
		return RESP{}, err
	}

	switch p {
	case '$':
		return parseBulkString(reader)
	case '+':
		return parseSimpleString(reader)
	case '*':
		return parseArray(reader)
	default:
		return RESP{}, fmt.Errorf("unknown RESP type: %c", p)
	}
}

func parseArray(reader *bufio.Reader) (RESP, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return RESP{}, err
	}
	count, err := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
	if err != nil {
		return RESP{}, err
	}

	var elements []RESP
	for i := 0; i < count; i++ {
		elem, err := parse(reader)
		if err != nil {
			return RESP{}, err
		}
		elements = append(elements, elem)
	}

	return RESP{Type: Array, Array: elements}, nil
}

func parseSimpleString(reader *bufio.Reader) (RESP, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return RESP{}, err
	}
	return RESP{Type: SimpleString, String: strings.TrimSuffix(line, "\r\n")}, nil
}

func parseBulkString(reader *bufio.Reader) (RESP, error) {

	line, err := reader.ReadString('\n')
	if err != nil {
		return RESP{}, err
	}

	length, err := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
	if err != nil {
		return RESP{}, err
	}

	if length == -1 {
		return RESP{Type: Null}, nil
	}

	bulkData := make([]byte, length+2)

	_, err = reader.Read(bulkData)
	if err != nil {
		return RESP{}, err
	}

	return RESP{Type: BulkString, String: string(bulkData[:length])}, nil
}

func handleCommand(object RESP) []byte {
	if object.Type != Array || len(object.Array) == 0 {
		return []byte("-ERR Invalid command format\r\n")
	}

	cmd := strings.ToUpper(object.Array[0].String)
	args := object.Array[1:]
	fmt.Println(args)

	switch cmd {
	case "PING":
		return []byte("+PONG\r\n")
	case "ECHO":
		if len(args) != 1 {
			return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
		}

		response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[0].String), args[0].String)
		return []byte(response)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}
