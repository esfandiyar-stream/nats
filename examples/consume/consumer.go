package main

import (
	"context"
	"encoding/json"
	"github.com/esfandiyar-stream/nats/gnats"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
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

	// Keep the program running
	select {}
}
