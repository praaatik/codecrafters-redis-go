package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

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
