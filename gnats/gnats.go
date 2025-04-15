package gnats

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type ConsumerRunner struct {
	consumers []*Consumer
	lg        *slog.Logger
	conn      *nats.Conn
	js        nats.JetStreamContext
}

func NewConsumerRunner(lg *slog.Logger, consumers ...*Consumer) *ConsumerRunner {
	return &ConsumerRunner{
		consumers: consumers,
		lg:        lg,
	}
}

func (cr *ConsumerRunner) RunInnerWorkers() {
	for _, c := range cr.consumers {
		c.RunInnerWorkers()
	}
}

func (cr *ConsumerRunner) Setup(conn *nats.Conn, js nats.JetStreamContext) error {
	cr.conn = conn
	cr.js = js

	for _, consumer := range cr.consumers {
		if err := consumer.Setup(conn, js); err != nil {
			cr.lg.Error("failed to setup consumer", "err", err)
			return err
		}
	}
	go cr.RunInnerWorkers()
	return cr.startWorkers()
}

func (cr *ConsumerRunner) startWorkers() error {
	for _, consumer := range cr.consumers {
		go consumer.Worker()
	}
	return nil
}

type NATS struct {
	conn             *nats.Conn
	js               nats.JetStreamContext
	connectionString string
	streamName       string
	lg               *slog.Logger
	connected        bool
	Mutex            sync.RWMutex
}

func NewNATS(connectionString, streamName string, lg *slog.Logger) *NATS {
	return &NATS{
		connectionString: connectionString,
		streamName:       streamName,
		lg:               lg,
	}
}

func (n *NATS) Setup(consumerRunner *ConsumerRunner) error {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()

	if err := n.cleanup(); err != nil {
		return err
	}

	if err := n.Connect(); err != nil {
		return err
	}

	if err := n.SetupJetStream(); err != nil {
		return err
	}

	if consumerRunner != nil {
		if err := consumerRunner.Setup(n.conn, n.js); err != nil {
			return err
		}
	}

	n.connected = true
	go n.handleDisconnect(consumerRunner)

	return nil
}

func (n *NATS) cleanup() error {
	if n.js != nil {
		// JetStream context is tied to connection, no separate cleanup needed
	}
	if n.conn != nil {
		n.conn.Close()
	}
	return nil
}

func (n *NATS) Connect() error {
	conn, err := nats.Connect(n.connectionString,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1*time.Second),
	)
	if err != nil {
		return err
	}
	n.conn = conn
	return nil
}

func (n *NATS) SetupJetStream() error {
	js, err := n.conn.JetStream()
	if err != nil {
		n.lg.Error("failed to initialize jetstream", "err", err)
		return err
	}
	n.js = js

	// Ensure the stream exists
	n.lg.Info("creating stream", "name", n.streamName)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     n.streamName,
		Subjects: []string{n.streamName + ".>"}, // e.g., "orders.>"
		Storage:  nats.FileStorage,
	})
	if err != nil {
		n.lg.Error("failed to create stream", "name", n.streamName, "err", err)
		if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			n.lg.Info("stream already exists", "name", n.streamName)
			return nil
		}
		return err
	}
	n.lg.Info("stream created successfully", "name", n.streamName)
	return nil
}

func (n *NATS) handleDisconnect(consumerRunner *ConsumerRunner) {
	for {
		if n.conn != nil && !n.conn.IsConnected() {
			n.connected = false
			n.lg.Error("NATS connection closed")

			backoff := 1 * time.Second
			maxBackoff := 30 * time.Second

			for !n.connected {
				n.lg.Info("attempting to reconnect to NATS", "backoff", backoff)
				time.Sleep(backoff)

				if err := n.Setup(consumerRunner); err != nil {
					n.lg.Error("failed to reconnect", "err", err)
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}

				n.lg.Info("successfully reconnected to NATS")
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (n *NATS) HealthCheck() error {
	n.Mutex.RLock()
	defer n.Mutex.RUnlock()

	if n.conn == nil || !n.conn.IsConnected() {
		return errors.New("nats connection is not open")
	}

	if n.js == nil {
		return errors.New("jetstream context is not initialized")
	}

	return nil
}

func (n *NATS) JetStream() nats.JetStreamContext {
	n.Mutex.RLock()
	defer n.Mutex.RUnlock()
	return n.js
}
