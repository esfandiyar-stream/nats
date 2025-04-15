package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/esfandiyar-stream/nats/gnats"
	"go.opentelemetry.io/otel"
	"log/slog"
)

type Order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

func main() {
	// Initialize logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	// Initialize OpenTelemetry meter
	meter := otel.Meter("nats-example")

	// Initialize NATS with JetStream
	natsBroker := gnats.NewNATS("nats://localhost:4222", "orders", logger)
	logger.Info("initializing consumer", "stream", "orders", "subject", "orders.created")

	// Wait for JetStream to be ready
	for {
		if err := natsBroker.HealthCheck(); err == nil {
			break
		}
		logger.Info("waiting for nats connection")
		time.Sleep(1 * time.Second)
	}

	// Create a consumer
	consumer, err := gnats.NewConsumer(
		logger,
		100,              // queueLength
		5,                // workersCount
		"orders.created", // subject
		"order-consumer", // consumerName
		gnats.WithHandlers(map[string]gnats.HandlerFunc{
			"orders.created": func(ctx context.Context, data []byte) error {
				var order Order
				if err := json.Unmarshal(data, &order); err != nil {
					logger.Error("failed to unmarshal order", "error", err)
					return err
				}
				logger.Info("processed order", "id", order.ID, "total", order.Total)
				return nil
			},
		}),
		gnats.WithOtelMetric(meter),
	)
	if err != nil {
		logger.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	// Set up consumer runner
	runner := gnats.NewConsumerRunner(logger, consumer)
	if err := natsBroker.Setup(runner); err != nil {
		logger.Error("failed to setup nats", "error", err)
		os.Exit(1)
	}
	logger.Info("consumer setup complete", "consumerName", "order-consumer")

	// Keep the program running
	select {}
}
