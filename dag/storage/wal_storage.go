package storage

import (
	"sync"
)

// WALMemoryTaskStorage implements TaskStorage with WAL support using memory storage
type WALMemoryTaskStorage struct {
	*MemoryTaskStorage
	walManager any // WAL manager interface to avoid import cycle
	walStorage any // WAL storage interface to avoid import cycle
	mu         sync.RWMutex
}

// WALSQLTaskStorage implements TaskStorage with WAL support using SQL storage
type WALSQLTaskStorage struct {
	*SQLTaskStorage
	walManager any // WAL manager interface to avoid import cycle
	walStorage any // WAL storage interface to avoid import cycle
	mu         sync.RWMutex
}
