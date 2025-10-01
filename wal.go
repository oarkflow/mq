package mq

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
)

// WALEntry represents a single write-ahead log entry
type WALEntry struct {
	EntryType  WALEntryType    `json:"entry_type"`
	TaskID     string          `json:"task_id"`
	QueueName  string          `json:"queue_name"`
	Timestamp  time.Time       `json:"timestamp"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	SequenceID int64           `json:"sequence_id"`
}

// WALEntryType defines the type of WAL entry
type WALEntryType string

const (
	WALEntryEnqueue    WALEntryType = "ENQUEUE"
	WALEntryDequeue    WALEntryType = "DEQUEUE"
	WALEntryComplete   WALEntryType = "COMPLETE"
	WALEntryFailed     WALEntryType = "FAILED"
	WALEntryCheckpoint WALEntryType = "CHECKPOINT"
)

// WriteAheadLog provides message durability through persistent logging
type WriteAheadLog struct {
	dir           string
	currentFile   *os.File
	currentWriter *bufio.Writer
	sequenceID    int64
	maxFileSize   int64
	syncInterval  time.Duration
	mu            sync.Mutex
	logger        logger.Logger
	shutdown      chan struct{}
	wg            sync.WaitGroup
	entries       chan *WALEntry
	fsyncOnWrite  bool
}

// WALConfig holds configuration for the WAL
type WALConfig struct {
	Directory    string
	MaxFileSize  int64         // Maximum file size before rotation
	SyncInterval time.Duration // Interval for syncing to disk
	FsyncOnWrite bool          // Sync after every write (slower but more durable)
	Logger       logger.Logger
}

// NewWriteAheadLog creates a new write-ahead log
func NewWriteAheadLog(config WALConfig) (*WriteAheadLog, error) {
	if config.MaxFileSize == 0 {
		config.MaxFileSize = 100 * 1024 * 1024 // 100MB default
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 1 * time.Second
	}

	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WriteAheadLog{
		dir:          config.Directory,
		maxFileSize:  config.MaxFileSize,
		syncInterval: config.SyncInterval,
		logger:       config.Logger,
		shutdown:     make(chan struct{}),
		entries:      make(chan *WALEntry, 10000),
		fsyncOnWrite: config.FsyncOnWrite,
	}

	// Recover sequence ID from existing logs
	if err := wal.recoverSequenceID(); err != nil {
		return nil, fmt.Errorf("failed to recover sequence ID: %w", err)
	}

	// Open or create current log file
	if err := wal.openNewFile(); err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Start background workers
	wal.wg.Add(2)
	go wal.writeWorker()
	go wal.syncWorker()

	return wal, nil
}

// WriteEntry writes a new entry to the WAL
func (w *WriteAheadLog) WriteEntry(ctx context.Context, entry *WALEntry) error {
	w.mu.Lock()
	w.sequenceID++
	entry.SequenceID = w.sequenceID
	entry.Timestamp = time.Now()
	w.mu.Unlock()

	select {
	case w.entries <- entry:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.shutdown:
		return fmt.Errorf("WAL is shutting down")
	}
}

// writeWorker processes WAL entries in the background
func (w *WriteAheadLog) writeWorker() {
	defer w.wg.Done()

	for {
		select {
		case entry := <-w.entries:
			if err := w.writeEntryToFile(entry); err != nil {
				w.logger.Error("Failed to write WAL entry",
					logger.Field{Key: "error", Value: err},
					logger.Field{Key: "taskID", Value: entry.TaskID})
			}
		case <-w.shutdown:
			// Drain remaining entries
			for len(w.entries) > 0 {
				entry := <-w.entries
				_ = w.writeEntryToFile(entry)
			}
			return
		}
	}
}

// writeEntryToFile writes a single entry to the current WAL file
func (w *WriteAheadLog) writeEntryToFile(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	// Write entry with newline delimiter
	if _, err := w.currentWriter.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	if w.fsyncOnWrite {
		if err := w.currentWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush WAL: %w", err)
		}
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	}

	// Check if we need to rotate the file
	stat, err := w.currentFile.Stat()
	if err == nil && stat.Size() >= w.maxFileSize {
		return w.rotateFile()
	}

	return nil
}

// syncWorker periodically syncs the WAL to disk
func (w *WriteAheadLog) syncWorker() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.currentWriter != nil {
				_ = w.currentWriter.Flush()
			}
			if w.currentFile != nil {
				_ = w.currentFile.Sync()
			}
			w.mu.Unlock()
		case <-w.shutdown:
			return
		}
	}
}

// openNewFile creates a new WAL file
func (w *WriteAheadLog) openNewFile() error {
	filename := fmt.Sprintf("wal-%d.log", time.Now().UnixNano())
	filepath := filepath.Join(w.dir, filename)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	w.currentFile = file
	w.currentWriter = bufio.NewWriter(file)

	w.logger.Info("Opened new WAL file", logger.Field{Key: "filename", Value: filename})

	return nil
}

// rotateFile rotates to a new WAL file
func (w *WriteAheadLog) rotateFile() error {
	// Flush and close current file
	if w.currentWriter != nil {
		if err := w.currentWriter.Flush(); err != nil {
			return err
		}
	}
	if w.currentFile != nil {
		if err := w.currentFile.Close(); err != nil {
			return err
		}
	}

	// Open new file
	return w.openNewFile()
}

// recoverSequenceID recovers the last sequence ID from existing WAL files
func (w *WriteAheadLog) recoverSequenceID() error {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return err
	}

	maxSeq := int64(0)

	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var entry WALEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				continue
			}
			if entry.SequenceID > maxSeq {
				maxSeq = entry.SequenceID
			}
		}

		file.Close()
	}

	w.sequenceID = maxSeq
	w.logger.Info("Recovered sequence ID", logger.Field{Key: "sequenceID", Value: maxSeq})

	return nil
}

// Replay replays all WAL entries to recover state
func (w *WriteAheadLog) Replay(handler func(*WALEntry) error) error {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}

	// Sort files by name (which includes timestamp)
	// Simple bubble sort since the list is typically small
	for i := 0; i < len(files)-1; i++ {
		for j := i + 1; j < len(files); j++ {
			if files[i] > files[j] {
				files[i], files[j] = files[j], files[i]
			}
		}
	}

	entriesReplayed := 0

	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			w.logger.Error("Failed to open WAL file for replay",
				logger.Field{Key: "error", Value: err},
				logger.Field{Key: "file", Value: filepath})
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var entry WALEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				w.logger.Error("Failed to unmarshal WAL entry",
					logger.Field{Key: "error", Value: err})
				continue
			}

			if err := handler(&entry); err != nil {
				w.logger.Error("Failed to replay WAL entry",
					logger.Field{Key: "error", Value: err},
					logger.Field{Key: "taskID", Value: entry.TaskID})
				continue
			}

			entriesReplayed++
		}

		file.Close()
	}

	w.logger.Info("WAL replay complete",
		logger.Field{Key: "entries", Value: entriesReplayed})

	return nil
}

// Checkpoint writes a checkpoint entry and optionally truncates old logs
func (w *WriteAheadLog) Checkpoint(ctx context.Context, state map[string]interface{}) error {
	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint state: %w", err)
	}

	entry := &WALEntry{
		EntryType: WALEntryCheckpoint,
		TaskID:    "checkpoint",
		Payload:   stateData,
	}

	return w.WriteEntry(ctx, entry)
}

// TruncateOldLogs removes old WAL files (called after checkpoint)
func (w *WriteAheadLog) TruncateOldLogs(keepRecent int) error {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}

	// Sort files by modification time
	type fileInfo struct {
		path    string
		modTime time.Time
	}

	var fileInfos []fileInfo
	for _, path := range files {
		stat, err := os.Stat(path)
		if err != nil {
			continue
		}
		fileInfos = append(fileInfos, fileInfo{path: path, modTime: stat.ModTime()})
	}

	// Sort by modification time (newest first)
	for i := 0; i < len(fileInfos)-1; i++ {
		for j := i + 1; j < len(fileInfos); j++ {
			if fileInfos[i].modTime.Before(fileInfos[j].modTime) {
				fileInfos[i], fileInfos[j] = fileInfos[j], fileInfos[i]
			}
		}
	}

	// Remove old files
	removed := 0
	for i := keepRecent; i < len(fileInfos); i++ {
		if err := os.Remove(fileInfos[i].path); err != nil {
			w.logger.Error("Failed to remove old WAL file",
				logger.Field{Key: "error", Value: err},
				logger.Field{Key: "file", Value: fileInfos[i].path})
			continue
		}
		removed++
	}

	w.logger.Info("Truncated old WAL files",
		logger.Field{Key: "removed", Value: removed})

	return nil
}

// Shutdown gracefully shuts down the WAL
func (w *WriteAheadLog) Shutdown(ctx context.Context) error {
	close(w.shutdown)

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.currentWriter != nil {
			_ = w.currentWriter.Flush()
		}
		if w.currentFile != nil {
			_ = w.currentFile.Sync()
			_ = w.currentFile.Close()
		}

		w.logger.Info("WAL shutdown complete")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetStats returns statistics about the WAL
func (w *WriteAheadLog) GetStats() map[string]interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	var currentFileSize int64
	if w.currentFile != nil {
		if stat, err := w.currentFile.Stat(); err == nil {
			currentFileSize = stat.Size()
		}
	}

	files, _ := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))

	return map[string]interface{}{
		"current_sequence_id": w.sequenceID,
		"current_file_size":   currentFileSize,
		"total_files":         len(files),
		"entries_backlog":     len(w.entries),
	}
}
