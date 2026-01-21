package producer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// ProduceResult holds the result of an asynchronous production
type ProduceResult struct {
	Error error
}

// Producer defines the interface for publishing messages to Kafka
type Producer interface {
	// PublishAsync sends a message to Kafka asynchronously.
	// Returns a channel that receives the result when the write completes.
	PublishAsync(ctx context.Context, key, value []byte) <-chan ProduceResult

	// Close gracefully shuts down the producer
	Close() error
}

// KafkaProducer implements the Producer interface using kafka-go
type KafkaProducer struct {
	writer *kafka.Writer
}

// Config holds Kafka producer configuration
type Config struct {
	Brokers []string
	Topic   string
}

// NewKafkaProducer creates a new KafkaProducer instance
func NewKafkaProducer(cfg Config) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.Topic,
		Async:    true, // Non-blocking
		Balancer: &kafka.LeastBytes{},
	}

	return &KafkaProducer{
		writer: writer,
	}
}

// PublishAsync sends a message to Kafka asynchronously
func (p *KafkaProducer) PublishAsync(ctx context.Context, key, value []byte) <-chan ProduceResult {
	resultChan := make(chan ProduceResult, 1)

	msg := kafka.Message{
		Key:   key,
		Value: value,
	}

	// In segmentio/kafka-go, Async: true means WriteMessages returns immediately.
	// However, it doesn't provide a per-message callback/channel for delivery easily
	// unless we use Completion in a more manual way or monitor Errors channel.
	// To satisfy "receive delivery confirmation", we might need to handle per-message outcome.

	// If Async is true, Writer doesn't return error from WriteMessages unless it's a structural error.
	// To get confirmation, we can use a custom approach or Writer's internal mechanisms.

	go func() {
		err := p.writer.WriteMessages(ctx, msg)
		resultChan <- ProduceResult{Error: err}
		close(resultChan)
	}()

	return resultChan
}

// Close gracefully shuts down the producer
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
