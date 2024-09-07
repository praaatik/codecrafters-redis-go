package main

import (
	"fmt"
	"net"
	"os"
)

var _ = net.Listen
var _ = os.Exit

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
	buf := make([]byte, 1024)

	_, err := conn.Read(buf)

	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		return
	}

	_, err = conn.Write([]byte("+PONG\r\n"))

	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
	}
}
