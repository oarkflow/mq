# WAL (Write-Ahead Logging) System

This directory contains a robust enterprise-grade WAL system implementation designed to prevent database overload from frequent task logging operations.

## Overview

The WAL system provides:
- **Buffered Logging**: High-frequency logging operations are buffered in memory
- **Batch Processing**: Periodic batch flushing to database for optimal performance
- **Crash Recovery**: Automatic recovery of unflushed entries on system restart
- **Performance Metrics**: Real-time monitoring of WAL operations
- **Graceful Shutdown**: Ensures data consistency during shutdown

## Key Components

### 1. WAL Manager (`dag/wal/wal.go`)
Core WAL functionality with buffering, segment management, and flush operations.

### 2. WAL Storage (`dag/wal/storage.go`)
Database persistence layer for WAL entries and segments.

### 3. WAL Recovery (`dag/wal/recovery.go`)
Crash recovery mechanisms to replay unflushed entries.

### 4. WAL Factory (`dag/wal_factory.go`)
Factory for creating WAL-enabled storage instances.

## Usage Example

```go
package main

import (
    "context"
    "time"

    "github.com/oarkflow/mq/dag"
    "github.com/oarkflow/mq/dag/storage"
    "github.com/oarkflow/mq/dag/wal"
    "github.com/oarkflow/mq/logger"
)

func main() {
    // Create logger
    l := logger.NewDefaultLogger()

    // Create WAL-enabled storage factory
    factory := dag.NewWALEnabledStorageFactory(l)

    // Configure WAL
    walConfig := &wal.WALConfig{
        MaxBufferSize:    5000,             // Buffer up to 5000 entries
        FlushInterval:    2 * time.Second,  // Flush every 2 seconds
        MaxFlushRetries:  3,                // Retry failed flushes
        MaxSegmentSize:   10000,            // 10K entries per segment
        SegmentRetention: 48 * time.Hour,   // Keep segments for 48 hours
        WorkerCount:      4,                // 4 flush workers
        BatchSize:        500,              // Batch 500 operations
        EnableRecovery:   true,             // Enable crash recovery
        EnableMetrics:    true,             // Enable metrics
    }

    // Create WAL-enabled storage
    storage, walManager, err := factory.CreateMemoryStorage(walConfig)
    if err != nil {
        panic(err)
    }
    defer storage.Close()

    // Create DAG with WAL-enabled storage
    d := dag.NewDAG("My DAG", "my-dag", func(taskID string, result mq.Result) {
        // Handle final results
    })

    // Set the WAL-enabled storage
    d.SetTaskStorage(storage)

    // Now all logging operations will be buffered and batched
    ctx := context.Background()

    // Create and log activities - these will be buffered
    for i := 0; i < 1000; i++ {
        task := &storage.PersistentTask{
            ID:     fmt.Sprintf("task-%d", i),
            DAGID:  "my-dag",
            Status: storage.TaskStatusRunning,
        }

        // This will be buffered, not written immediately to DB
        d.GetTaskStorage().SaveTask(ctx, task)

        // Activity logging will also be buffered
        activity := &storage.TaskActivityLog{
            TaskID:  task.ID,
            DAGID:   "my-dag",
            Action:  "processing",
            Message: "Task is being processed",
        }
        d.GetTaskStorage().LogActivity(ctx, activity)
    }

    // Get performance metrics
    metrics := walManager.GetMetrics()
    fmt.Printf("Buffered: %d, Flushed: %d\n", metrics.EntriesBuffered, metrics.EntriesFlushed)
}
```

## Configuration Options

### WALConfig Fields

- `MaxBufferSize`: Maximum entries to buffer before flush (default: 1000)
- `FlushInterval`: How often to flush buffer (default: 5s)
- `MaxFlushRetries`: Max retries for failed flushes (default: 3)
- `MaxSegmentSize`: Maximum entries per segment (default: 5000)
- `SegmentRetention`: How long to keep flushed segments (default: 24h)
- `WorkerCount`: Number of flush workers (default: 2)
- `BatchSize`: Batch size for database operations (default: 100)
- `EnableRecovery`: Enable crash recovery (default: true)
- `RecoveryTimeout`: Timeout for recovery operations (default: 30s)
- `EnableMetrics`: Enable metrics collection (default: true)
- `MetricsInterval`: Metrics collection interval (default: 10s)

## Performance Benefits

1. **Reduced Database Load**: Buffering prevents thousands of individual INSERT operations
2. **Batch Processing**: Database operations are performed in optimized batches
3. **Async Processing**: Logging doesn't block main application flow
4. **Configurable Buffering**: Tune buffer size based on your throughput needs
5. **Crash Recovery**: Never lose data even if system crashes

## Integration with Task Manager

The WAL system integrates seamlessly with the existing task manager:

```go
// The task manager will automatically use WAL buffering
// when WAL-enabled storage is configured
taskManager := NewTaskManager(dag, taskID, resultCh, iterators, walStorage)

// All activity logging will be buffered
taskManager.logActivity(ctx, "processing", "Task started processing")
```

## Monitoring

Get real-time metrics about WAL performance:

```go
metrics := walManager.GetMetrics()
fmt.Printf("Entries Buffered: %d\n", metrics.EntriesBuffered)
fmt.Printf("Entries Flushed: %d\n", metrics.EntriesFlushed)
fmt.Printf("Flush Operations: %d\n", metrics.FlushOperations)
fmt.Printf("Average Flush Time: %v\n", metrics.AverageFlushTime)
```

## Best Practices

1. **Tune Buffer Size**: Set based on your expected logging frequency
2. **Monitor Metrics**: Keep an eye on buffer usage and flush performance
3. **Configure Retention**: Set appropriate segment retention for your needs
4. **Use Recovery**: Always enable recovery for production deployments
5. **Batch Size**: Optimize batch size based on your database capabilities

## Database Support

The WAL system supports:
- PostgreSQL
- SQLite
- MySQL (via storage interface)
- In-memory storage (for testing/development)

## Error Handling

The WAL system includes comprehensive error handling:
- Failed flushes are automatically retried
- Recovery process validates entries before replay
- Graceful degradation if storage is unavailable
- Detailed logging for troubleshooting
