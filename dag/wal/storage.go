package wal

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq/dag/storage"
)

// WALStorageImpl implements WALStorage interface
type WALStorageImpl struct {
	underlying storage.TaskStorage
	db         *sql.DB
	config     *WALConfig

	// WAL tables
	walEntriesTable  string
	walSegmentsTable string

	mu sync.RWMutex
}

// NewWALStorage creates a new WAL-enabled storage wrapper
func NewWALStorage(underlying storage.TaskStorage, db *sql.DB, config *WALConfig) *WALStorageImpl {
	if config == nil {
		config = DefaultWALConfig()
	}

	return &WALStorageImpl{
		underlying:       underlying,
		db:               db,
		config:           config,
		walEntriesTable:  "wal_entries",
		walSegmentsTable: "wal_segments",
	}
}

// InitializeTables creates the necessary WAL tables
func (ws *WALStorageImpl) InitializeTables(ctx context.Context) error {
	// Create WAL entries table
	entriesQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(255) PRIMARY KEY,
			type VARCHAR(50) NOT NULL,
			timestamp TIMESTAMP NOT NULL,
			sequence_id BIGINT NOT NULL,
			data JSONB,
			metadata JSONB,
			checksum VARCHAR(255),
			segment_id VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(sequence_id)
		)`, ws.walEntriesTable)

	// Create WAL segments table
	segmentsQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(255) PRIMARY KEY,
			start_seq_id BIGINT NOT NULL,
			end_seq_id BIGINT NOT NULL,
			status VARCHAR(20) NOT NULL DEFAULT 'active',
			checksum VARCHAR(255),
			created_at TIMESTAMP NOT NULL,
			flushed_at TIMESTAMP,
			INDEX idx_status (status),
			INDEX idx_created_at (created_at),
			INDEX idx_sequence_range (start_seq_id, end_seq_id)
		)`, ws.walSegmentsTable)

	// Execute table creation
	if _, err := ws.db.ExecContext(ctx, entriesQuery); err != nil {
		return fmt.Errorf("failed to create WAL entries table: %w", err)
	}

	if _, err := ws.db.ExecContext(ctx, segmentsQuery); err != nil {
		return fmt.Errorf("failed to create WAL segments table: %w", err)
	}

	// Create indexes for better performance
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_wal_entries_sequence ON %s (sequence_id)", ws.walEntriesTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_wal_entries_timestamp ON %s (timestamp)", ws.walEntriesTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_wal_entries_segment ON %s (segment_id)", ws.walEntriesTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_wal_segments_status ON %s (status)", ws.walSegmentsTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_wal_segments_range ON %s (start_seq_id, end_seq_id)", ws.walSegmentsTable),
	}

	for _, idx := range indexes {
		if _, err := ws.db.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// SaveWALEntry saves a single WAL entry
func (ws *WALStorageImpl) SaveWALEntry(ctx context.Context, entry *WALEntry) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (id, type, timestamp, sequence_id, data, metadata, checksum, segment_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (sequence_id) DO NOTHING`, ws.walEntriesTable)

	metadataJSON, _ := json.Marshal(entry.Metadata)

	_, err := ws.db.ExecContext(ctx, query,
		entry.ID,
		string(entry.Type),
		entry.Timestamp,
		entry.SequenceID,
		string(entry.Data),
		string(metadataJSON),
		entry.Checksum,
		entry.ID, // Use entry ID as segment ID for single entries
	)

	if err != nil {
		return fmt.Errorf("failed to save WAL entry: %w", err)
	}

	return nil
}

// SaveWALEntries saves multiple WAL entries in a batch
func (ws *WALStorageImpl) SaveWALEntries(ctx context.Context, entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := ws.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
		INSERT INTO %s (id, type, timestamp, sequence_id, data, metadata, checksum, segment_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (sequence_id) DO NOTHING`, ws.walEntriesTable)

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, entry := range entries {
		metadataJSON, _ := json.Marshal(entry.Metadata)

		_, err = stmt.ExecContext(ctx,
			entry.ID,
			string(entry.Type),
			entry.Timestamp,
			entry.SequenceID,
			string(entry.Data),
			string(metadataJSON),
			entry.Checksum,
			entry.ID, // Use entry ID as segment ID
		)
		if err != nil {
			return fmt.Errorf("failed to save WAL entry %s: %w", entry.ID, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// SaveWALSegment saves a complete WAL segment
func (ws *WALStorageImpl) SaveWALSegment(ctx context.Context, segment *WALSegment) error {
	tx, err := ws.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Save segment metadata
	segmentQuery := fmt.Sprintf(`
		INSERT INTO %s (id, start_seq_id, end_seq_id, status, checksum, created_at, flushed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			flushed_at = EXCLUDED.flushed_at`, ws.walSegmentsTable)

	var flushedAt any
	if segment.FlushedAt != nil {
		flushedAt = *segment.FlushedAt
	} else {
		flushedAt = nil
	}

	_, err = tx.ExecContext(ctx, segmentQuery,
		segment.ID,
		segment.StartSeqID,
		segment.EndSeqID,
		string(segment.Status),
		segment.Checksum,
		segment.CreatedAt,
		flushedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save WAL segment: %w", err)
	}

	// Save all entries in the segment
	if len(segment.Entries) > 0 {
		entriesQuery := fmt.Sprintf(`
			INSERT INTO %s (id, type, timestamp, sequence_id, data, metadata, checksum, segment_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (sequence_id) DO NOTHING`, ws.walEntriesTable)

		stmt, err := tx.PrepareContext(ctx, entriesQuery)
		if err != nil {
			return fmt.Errorf("failed to prepare entries statement: %w", err)
		}
		defer stmt.Close()

		for _, entry := range segment.Entries {
			metadataJSON, _ := json.Marshal(entry.Metadata)

			_, err = stmt.ExecContext(ctx,
				entry.ID,
				string(entry.Type),
				entry.Timestamp,
				entry.SequenceID,
				string(entry.Data),
				string(metadataJSON),
				entry.Checksum,
				segment.ID,
			)
			if err != nil {
				return fmt.Errorf("failed to save entry %s: %w", entry.ID, err)
			}
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit segment transaction: %w", err)
	}

	return nil
}

// GetWALSegments retrieves WAL segments since a given time
func (ws *WALStorageImpl) GetWALSegments(ctx context.Context, since time.Time) ([]WALSegment, error) {
	query := fmt.Sprintf(`
		SELECT id, start_seq_id, end_seq_id, status, checksum, created_at, flushed_at
		FROM %s
		WHERE created_at >= ?
		ORDER BY created_at ASC`, ws.walSegmentsTable)

	rows, err := ws.db.QueryContext(ctx, query, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query WAL segments: %w", err)
	}
	defer rows.Close()

	var segments []WALSegment
	for rows.Next() {
		var segment WALSegment
		var flushedAt sql.NullTime

		err := rows.Scan(
			&segment.ID,
			&segment.StartSeqID,
			&segment.EndSeqID,
			&segment.Status,
			&segment.Checksum,
			&segment.CreatedAt,
			&flushedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan segment: %w", err)
		}

		if flushedAt.Valid {
			segment.FlushedAt = &flushedAt.Time
		}

		segments = append(segments, segment)
	}

	return segments, rows.Err()
}

// GetUnflushedEntries retrieves entries that haven't been applied yet
func (ws *WALStorageImpl) GetUnflushedEntries(ctx context.Context) ([]WALEntry, error) {
	query := fmt.Sprintf(`
		SELECT we.id, we.type, we.timestamp, we.sequence_id, we.data, we.metadata, we.checksum
		FROM %s we
		LEFT JOIN %s ws ON we.segment_id = ws.id
		WHERE ws.status IN ('active', 'flushing') OR ws.id IS NULL
		ORDER BY we.sequence_id ASC`, ws.walEntriesTable, ws.walSegmentsTable)

	rows, err := ws.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query unflushed entries: %w", err)
	}
	defer rows.Close()

	var entries []WALEntry
	for rows.Next() {
		var entry WALEntry
		var metadata sql.NullString

		err := rows.Scan(
			&entry.ID,
			&entry.Type,
			&entry.Timestamp,
			&entry.SequenceID,
			&entry.Data,
			&metadata,
			&entry.Checksum,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan entry: %w", err)
		}

		if metadata.Valid && metadata.String != "" {
			json.Unmarshal([]byte(metadata.String), &entry.Metadata)
		}

		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

// DeleteOldSegments deletes WAL segments older than the specified time
func (ws *WALStorageImpl) DeleteOldSegments(ctx context.Context, olderThan time.Time) error {
	// First, delete entries from old segments
	deleteEntriesQuery := fmt.Sprintf(`
		DELETE FROM %s
		WHERE segment_id IN (
			SELECT id FROM %s
			WHERE status = 'flushed' AND flushed_at < ?
		)`, ws.walEntriesTable, ws.walSegmentsTable)

	// Then delete the segments themselves
	deleteSegmentsQuery := fmt.Sprintf(`
		DELETE FROM %s
		WHERE status = 'flushed' AND flushed_at < ?`, ws.walSegmentsTable)

	tx, err := ws.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete entries
	if _, err = tx.ExecContext(ctx, deleteEntriesQuery, olderThan); err != nil {
		return fmt.Errorf("failed to delete old entries: %w", err)
	}

	// Delete segments
	if _, err = tx.ExecContext(ctx, deleteSegmentsQuery, olderThan); err != nil {
		return fmt.Errorf("failed to delete old segments: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit cleanup transaction: %w", err)
	}

	return nil
}

// SaveTask delegates to underlying storage
func (ws *WALStorageImpl) SaveTask(ctx context.Context, task *storage.PersistentTask) error {
	return ws.underlying.SaveTask(ctx, task)
}

// LogActivity delegates to underlying storage
func (ws *WALStorageImpl) LogActivity(ctx context.Context, log *storage.TaskActivityLog) error {
	return ws.underlying.LogActivity(ctx, log)
}

// WALEnabledStorage wraps any TaskStorage with WAL functionality
type WALEnabledStorage struct {
	storage.TaskStorage
	walManager *WALManager
}

// NewWALEnabledStorage creates a new WAL-enabled storage wrapper
func NewWALEnabledStorage(underlying storage.TaskStorage, walManager *WALManager) *WALEnabledStorage {
	return &WALEnabledStorage{
		TaskStorage: underlying,
		walManager:  walManager,
	}
}

// SaveTask saves a task through WAL
func (wes *WALEnabledStorage) SaveTask(ctx context.Context, task *storage.PersistentTask) error {
	// Serialize task to JSON for WAL
	taskData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Write to WAL first
	if err := wes.walManager.WriteEntry(ctx, WALEntryTypeTaskUpdate, taskData, map[string]any{
		"task_id": task.ID,
		"dag_id":  task.DAGID,
	}); err != nil {
		return fmt.Errorf("failed to write task to WAL: %w", err)
	}

	// Delegate to underlying storage (will be applied during flush)
	return wes.TaskStorage.SaveTask(ctx, task)
}

// LogActivity logs activity through WAL
func (wes *WALEnabledStorage) LogActivity(ctx context.Context, log *storage.TaskActivityLog) error {
	// Serialize log to JSON for WAL
	logData, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("failed to marshal activity log: %w", err)
	}

	// Write to WAL first
	if err := wes.walManager.WriteEntry(ctx, WALEntryTypeActivityLog, logData, map[string]any{
		"task_id": log.TaskID,
		"dag_id":  log.DAGID,
		"action":  log.Action,
	}); err != nil {
		return fmt.Errorf("failed to write activity log to WAL: %w", err)
	}

	// Delegate to underlying storage (will be applied during flush)
	return wes.TaskStorage.LogActivity(ctx, log)
}
