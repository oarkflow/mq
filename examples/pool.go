package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	v1 "github.com/oarkflow/mq/v1"
)

func main() {
	storage := v1.NewInMemoryTaskStorage()
	pool := v1.NewPool(5,
		v1.WithTaskStorage(storage),
		v1.WithHandler(func(ctx context.Context, payload *v1.Task) v1.Result {
			v1.Logger.Info().Str("taskID", payload.ID).Msg("Processing task payload")
			time.Sleep(500 * time.Millisecond)
			return v1.Result{}
		}),
		v1.WithPoolCallback(func(ctx context.Context, result v1.Result) error {
			v1.Logger.Info().Msg("Task callback invoked")
			return nil
		}),
		v1.WithCircuitBreaker(v1.CircuitBreakerConfig{
			Enabled:          true,
			FailureThreshold: 3,
			ResetTimeout:     5 * time.Second,
		}),
		v1.WithWarningThresholds(v1.ThresholdConfig{
			HighMemory:    v1.Config.WarningThreshold.HighMemory,
			LongExecution: v1.Config.WarningThreshold.LongExecution,
		}),
		v1.WithDiagnostics(true),
		v1.WithMetricsRegistry(v1.NewInMemoryMetricsRegistry()),
		v1.WithGracefulShutdown(10*time.Second),
		v1.WithPlugin(&v1.DefaultPlugin{}),
	)
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("Payload %d", i)
		task := &v1.Task{Payload: payload}
		if err := pool.EnqueueTask(context.Background(), task, rand.Intn(10)); err != nil {
			v1.Logger.Error().Err(err).Msg("Failed to enqueue task")
		}
	}
	time.Sleep(5 * time.Second)
	metrics := pool.Metrics()
	v1.Logger.Info().Msgf("Metrics: %+v", metrics)
	pool.Stop()
	v1.Logger.Info().Msgf("Dead Letter Queue has %d tasks", len(v1.DLQ.Task()))
}
