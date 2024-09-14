package main

import (
	"sync"
	"time"
)

type RESP struct {
	Type   RESPType
	String string
	Int    int
	Array  []RESP
}

type RedisServer struct {
	store   map[string]string
	streams map[string][]StreamEntry

	expiry map[string]time.Time
	mu     sync.RWMutex
}

type StreamEntry struct {
	ID        string
	KeyValues map[string]string
}
