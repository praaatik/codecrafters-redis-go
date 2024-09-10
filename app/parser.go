package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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

func (r *RedisServer) readRDBFile() (map[string]string, error) {
	if len(os.Args) < 4 {
		return nil, errors.New("not enough arguments")
	}

	file, err := os.Open(fmt.Sprintf("%s/%s", os.Args[2], os.Args[4]))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	keys := make(map[string]string)
	reader := bufio.NewReader(file)
	stat, _ := file.Stat()

	data := make([]byte, stat.Size())
	_, err = reader.Read(data)
	if err != nil {
		fmt.Println("error reading file -> ", err.Error())
		return nil, err
	}

	magicNumber := data[0:5]
	if string(magicNumber) != "REDIS" {
		return nil, fmt.Errorf("Invalid RDB file format")
	}

	offset := 9 // skipping the header -> "REDIS0011"
	var expiryTimestamp int64
	var expiryDuration time.Duration
	isExpiryFlag := false

	for offset < len(data) {
		switch data[offset] {
		case 0xFA: // auxiliary fields
			offset++
			expiryDuration = 0
			_, offset = parseStringEncodedValue(data, offset)
			_, offset = parseStringEncodedValue(data, offset)

		case 0xFE: // database selector
			expiryDuration = 0
			offset++
			parseSizeEncodedValue(data, offset)

		case 0xFF: // end of file
			fmt.Println("End of file.")
			return keys, nil

		case 0xFD: // expiry in seconds
			expiryTimestamp = int64(binary.LittleEndian.Uint32(data[offset+1 : offset+5]))
			offset += 5
			expiryDuration = time.Until(time.Unix(expiryTimestamp, 0))
			isExpiryFlag = true

		case 0xFC:
			expiryTimestamp = int64(binary.LittleEndian.Uint64(data[offset+1 : offset+9]))
			offset += 9
			expiryDuration = time.Until(time.UnixMilli(expiryTimestamp))
			isExpiryFlag = true

		default:
			valueType := data[offset]
			offset++

			if valueType == 0x00 {
				key, offset := parseStringEncodedValue(data, offset)
				value, offset := parseStringEncodedValue(data, offset)

				if key != "" && value != "" {
					if isExpiryFlag && expiryDuration <= 0 {
						isExpiryFlag = false
						continue
					}

					r.mu.Lock()
					r.store[key] = value
					if isExpiryFlag {
						r.expiry[key] = time.Now().Add(expiryDuration)
						isExpiryFlag = false
					}
					r.mu.Unlock()

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
