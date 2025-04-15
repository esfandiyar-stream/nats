package gnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type HandlerFunc func(context.Context, []byte) error
type SetupFunc func(conn *nats.Conn, js nats.JetStreamContext) error

type ConsumerConfig func(*Consumer) error

type Consumer struct {
	name           string
	subject        string
	consumerName   string
	delivery       *nats.Subscription
	logger         *slog.Logger
	handlers       map[string]HandlerFunc
	msgsQueue      chan *nats.Msg
	queueLength    int
	workersCount   int
	js             nats.JetStreamContext
	consumeCounter metric.Int64Counter
	successCounter metric.Int64Counter
	failCounter    metric.Int64Counter
	tracer         trace.Tracer
}

func WithSubject(subject string) ConsumerConfig {
	return func(c *Consumer) error {
		c.subject = subject
		return nil
	}
}

func WithHandlers(handlers map[string]HandlerFunc) ConsumerConfig {
	return func(c *Consumer) error {
		c.handlers = handlers
		return nil
	}
}

func WithOtelMetric(meter metric.Meter) ConsumerConfig {
	return func(c *Consumer) error {
		var err error
		c.consumeCounter, err = meter.Int64Counter(fmt.Sprintf("%s.consume.total.counter", c.name))
		if err != nil {
			return err
		}
		c.successCounter, err = meter.Int64Counter(fmt.Sprintf("%s.consume.success.counter", c.name))
		if err != nil {
			return err
		}
		c.failCounter, err = meter.Int64Counter(fmt.Sprintf("%s.consume.failed.counter", c.name))
		if err != nil {
			return err
		}
		return nil
	}
}

func WithTracer(tracer trace.Tracer) ConsumerConfig {
	return func(c *Consumer) error {
		c.tracer = tracer
		return nil
	}
}

type Carrier nats.Header

func (c Carrier) Get(key string) string {
	v := c[key]
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (c Carrier) Set(key, value string) {
	c[key] = []string{value}
}

func (c Carrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func NewConsumer(
	l *slog.Logger,
	queueLength int,
	workersCount int,
	subject string,
	consumerName string,
	configs ...ConsumerConfig,
) (*Consumer, error) {
	ec := &Consumer{
		name:         consumerName,
		subject:      subject,
		consumerName: consumerName,
		logger:       l.With("layer", "Consumer"),
		handlers:     make(map[string]HandlerFunc),
		queueLength:  queueLength,
		workersCount: workersCount,
		msgsQueue:    make(chan *nats.Msg, queueLength),
	}

	for _, cfg := range configs {
		if err := cfg(ec); err != nil {
			return nil, err
		}
	}

	return ec, nil
}

func (c *Consumer) RunInnerWorkers() {
	for i := 0; i < c.workersCount; i++ {
		go c.innerWorker()
	}
}

func (c *Consumer) Setup(conn *nats.Conn, js nats.JetStreamContext) error {
	c.js = js
	c.logger.Info("setting up consumer", "subject", c.subject, "consumerName", c.consumerName)

	// Create or update a durable consumer
	_, err := js.AddConsumer(c.subject, &nats.ConsumerConfig{
		Durable:   c.consumerName,
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		c.logger.Error("failed to create consumer", "err", err)
		return err
	}
	c.logger.Info("consumer created or exists", "consumerName", c.consumerName)

	// Subscribe using JetStream push-based consumer
	sub, err := js.Subscribe(c.subject, func(msg *nats.Msg) {
		c.logger.Debug("message received", "subject", msg.Subject)
		c.msgsQueue <- msg
	}, nats.Durable(c.consumerName), nats.ManualAck(), nats.AckWait(30*time.Second))
	if err != nil {
		c.logger.Error("failed to subscribe", "err", err)
		return err
	}
	c.delivery = sub
	c.logger.Info("subscribed to subject", "subject", c.subject)

	if c.tracer == nil {
		c.tracer = otel.Tracer("nats/consumer")
	}
	return nil
}

func (c *Consumer) RegisterHandler(routingKey string, handler HandlerFunc) {
	c.handlers[routingKey] = handler
}

func (c *Consumer) Worker() {
	c.logger.Info("started consumer worker", "name", c.name)
	select {} // Worker is driven by subscription callback
}

func (c *Consumer) innerWorker() {
	lg := c.logger.With("method", "InnerWorker")
	lg.Info("started consumer inner worker")

	defer func() {
		if r := recover(); r != nil {
			lg.Error("recovered from panic", "panic recovery", r)
		}
	}()
	propagator := otel.GetTextMapPropagator()
	for msg := range c.msgsQueue {
		lg.Debug("processing message from queue", "subject", msg.Subject)
		func() {
			routingKey := msg.Subject
			lg.Info("nats message received", "subject", routingKey)

			ctx := context.Background()
			eCtx := propagator.Extract(ctx, Carrier(msg.Header))
			spanCtx := trace.SpanContextFromContext(eCtx)
			bags := baggage.FromContext(eCtx)
			ctx = baggage.ContextWithBaggage(ctx, bags)
			ctx, span := c.tracer.Start(
				trace.ContextWithRemoteSpanContext(ctx, spanCtx),
				"nats_message",
				trace.WithSpanKind(trace.SpanKindConsumer),
			)
			defer span.End()

			ctx, cancel := context.WithTimeout(ctx, time.Second*55)
			defer cancel()

			if c.consumeCounter != nil {
				c.consumeCounter.Add(ctx, 1)
			}
			handler, ok := c.handlers[routingKey]
			if !ok {
				lg.Warn("no handler found for subject", "subject", routingKey)
				if c.failCounter != nil {
					c.failCounter.Add(ctx, 1)
				}
				if err := msg.Ack(); err != nil {
					lg.Error("failed to ack message", "error", err)
					recordTraceError(err, span)
				}
				lg.Info("nats message acked (no handler found)", "subject", routingKey)
				return
			}

			if err := handler(ctx, msg.Data); err == nil {
				if c.successCounter != nil {
					c.successCounter.Add(ctx, 1)
				}
				if err := msg.Ack(); err != nil {
					lg.Error("failed to ack message", "error", err)
					recordTraceError(err, span)
				}
				lg.Info("nats message acked", "subject", routingKey)
			} else {
				if c.failCounter != nil {
					c.failCounter.Add(ctx, 1)
				}
				if err := msg.Nak(); err != nil {
					lg.Error("failed to nack message", "error", err)
					recordTraceError(err, span)
				}
				lg.Warn("nats message nacked", "subject", routingKey)
			}
		}()
	}
}

func recordTraceError(err error, span trace.Span) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
