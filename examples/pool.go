package main

import (
	"context"
	"fmt"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/json"

	v1 "github.com/oarkflow/mq"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool := v1.NewPool(5,
		v1.WithTaskStorage(v1.NewMemoryTaskStorage(10*time.Minute)),
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
	defer func() {
		metrics := pool.Metrics()
		v1.Logger.Info().Msgf("Metrics: %+v", metrics)
		pool.Stop()
		v1.Logger.Info().Msgf("Dead Letter Queue has %d tasks", len(pool.DLQ.Task()))
	}()

	go func() {
		for i := 0; i < 50; i++ {
			task := &v1.Task{
				ID:      "",
				Payload: json.RawMessage(fmt.Sprintf("Task Payload %d", i)),
			}
			if err := pool.EnqueueTask(context.Background(), task, rand.Intn(10)); err != nil {
				v1.Logger.Error().Err(err).Msg("Failed to enqueue task")
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	<-ctx.Done()
	v1.Logger.Info().Msg("Received shutdown signal, exiting...")
}
