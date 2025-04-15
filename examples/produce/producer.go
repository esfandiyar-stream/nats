package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/esfandiyar-stream/nats/gnats"
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

	// Initialize NATS with JetStream
	natsBroker := gnats.NewNATS("nats://localhost:4222", "orders", logger)

	// Set up NATS connection
	if err := natsBroker.Setup(nil); err != nil {
		logger.Error("failed to setup nats", "error", err)
		os.Exit(1)
	}

	// Wait for JetStream to be ready
	for {
		if err := natsBroker.HealthCheck(); err == nil {
			break
		}
		logger.Info("waiting for nats connection")
		time.Sleep(1 * time.Second)
	}

	// Use the JetStream context via exported method
	js := natsBroker.JetStream()
	if js == nil {
		logger.Error("jetstream context is not initialized")
		os.Exit(1)
	}

	// Publish messages periodically
	for i := 1; ; i++ {
		order := Order{
			ID:    fmt.Sprintf("order-%d", i),
			Total: i * 100,
		}
		data, err := json.Marshal(order)
		if err != nil {
			logger.Error("failed to marshal order", "error", err)
			continue
		}

		// Publish to JetStream subject
		_, err = js.Publish("orders.created", data)
		if err != nil {
			logger.Error("failed to publish message", "error", err)
		} else {
			logger.Info("published order", "id", order.ID)
		}

		time.Sleep(2 * time.Second)
	}
}
