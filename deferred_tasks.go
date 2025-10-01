package mq

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/mq/consts"
)

// processDeferredTasks continuously checks for deferred tasks that are ready to be executed
func (b *Broker) processDeferredTasks(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopDeferredChan:
			return
		case <-ticker.C:
			now := time.Now()
			var tasksToProcess []*QueuedTask
			var taskIDsToRemove []string

			// Collect tasks that are ready to be processed
			b.deferredTasks.ForEach(func(taskID string, queuedTask *QueuedTask) bool {
				if queuedTask != nil && queuedTask.Task != nil {
					if queuedTask.Task.DeferUntil.Before(now) || queuedTask.Task.DeferUntil.Equal(now) {
						tasksToProcess = append(tasksToProcess, queuedTask)
						taskIDsToRemove = append(taskIDsToRemove, taskID)
					}
				}
				return true
			})

			// Process and remove ready tasks
			for i, task := range tasksToProcess {
				if task != nil && task.Message != nil {
					queueName := task.Message.Queue
					if queue, ok := b.queues.Get(queueName); ok {
						// Send task to the queue for processing
						select {
						case queue.tasks <- task:
							log.Printf("[DEFERRED] Task %s is now ready for processing on queue %s",
								task.Task.ID, queueName)
							b.deferredTasks.Del(taskIDsToRemove[i])
						default:
							// Queue is full, keep in deferred state
							log.Printf("[DEFERRED] Queue %s is full, task %s will retry",
								queueName, task.Task.ID)
						}
					} else {
						log.Printf("[DEFERRED] Queue %s not found for task %s",
							queueName, task.Task.ID)
						b.deferredTasks.Del(taskIDsToRemove[i])
					}
				}
			}
		}
	}
}

// StartDeferredTaskProcessor starts the background processor for deferred tasks
func (b *Broker) StartDeferredTaskProcessor(ctx context.Context) {
	go b.processDeferredTasks(ctx)
	log.Println("[DEFERRED] Deferred task processor started")
}

// StopDeferredTaskProcessor stops the deferred task processor
func (b *Broker) StopDeferredTaskProcessor() {
	close(b.stopDeferredChan)
	log.Println("[DEFERRED] Deferred task processor stopped")
}

// AddDeferredTask adds a task to the deferred queue
func (b *Broker) AddDeferredTask(task *QueuedTask) {
	if task != nil && task.Task != nil {
		b.deferredTasks.Set(task.Task.ID, task)
		log.Printf("[DEFERRED] Task %s deferred until %s",
			task.Task.ID, task.Task.DeferUntil.Format(time.RFC3339))
	}
}

// CreateDLQConsumer creates a consumer for a DLQ queue with a retry handler
func (b *Broker) CreateDLQConsumer(ctx context.Context, queueName string, retryHandler Handler) error {
	dlqName := queueName + "_dlq"

	// Check if DLQ exists
	dlq, ok := b.deadLetter.Get(queueName)
	if !ok {
		return nil // No DLQ for this queue
	}

	// Create a consumer for the DLQ
	consumerID := "dlq-consumer-" + queueName

	// Define DLQ processing handler
	dlqHandler := func(ctx context.Context, task *Task) Result {
		log.Printf("[DLQ] Processing task %s from DLQ %s (attempt %d)",
			task.ID, dlqName, task.Retries+1)

		// Call custom retry handler if provided
		if retryHandler != nil {
			result := retryHandler(ctx, task)
			if result.Status == Completed {
				log.Printf("[DLQ] Task %s successfully reprocessed from DLQ", task.ID)
				return result
			}
		}

		// Default behavior: retry with exponential backoff
		task.Retries++
		if task.Retries < task.MaxRetries {
			// Defer for exponential backoff
			backoffDuration := time.Duration(1<<uint(task.Retries)) * time.Second
			task.DeferUntil = time.Now().Add(backoffDuration)

			log.Printf("[DLQ] Task %s will be retried in %s", task.ID, backoffDuration)
			return Result{
				Status:  Processing,
				TaskID:  task.ID,
				Ctx:     ctx,
				Payload: task.Payload,
			}
		}

		// Max retries exceeded, mark as permanently failed
		log.Printf("[DLQ] Task %s exceeded max retries, permanently failed", task.ID)
		return Result{
			Status:  Failed,
			TaskID:  task.ID,
			Ctx:     ctx,
			Payload: task.Payload,
			Error:   task.Error,
		}
	}

	// Register the DLQ handler
	b.dlqHandlers.Set(dlqName, dlqHandler)

	// Create a consumer struct for the DLQ
	dlqConsumer := &consumer{
		id:    consumerID,
		state: consts.ConsumerStateActive,
		conn:  nil, // Internal consumer, no network connection
	}

	dlq.consumers.Set(consumerID, dlqConsumer)
	b.consumers.Set(consumerID, dlqConsumer)

	log.Printf("[DLQ] Consumer %s created for DLQ %s", consumerID, dlqName)
	return nil
}

// SetupDLQConsumers sets up consumers for all DLQ queues
func (b *Broker) SetupDLQConsumers(ctx context.Context, retryHandler Handler) {
	b.deadLetter.ForEach(func(queueName string, dlq *Queue) bool {
		// Extract original queue name (remove _dlq suffix)
		originalQueue := queueName
		if len(queueName) > 4 {
			originalQueue = queueName[:len(queueName)-4]
		}

		err := b.CreateDLQConsumer(ctx, originalQueue, retryHandler)
		if err != nil {
			log.Printf("[DLQ] Failed to create consumer for DLQ %s: %v", queueName, err)
		}
		return true
	})
	log.Println("[DLQ] DLQ consumers setup complete")
}
