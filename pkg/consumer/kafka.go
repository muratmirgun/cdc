package consumer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Message represents a message consumed from Kafka
type Message struct {
	Key    []byte
	Value  []byte
	Offset int64
	Topic  string
	Raw    kafka.Message // Keep raw for committing
}

// Consumer defines the interface for consuming messages from Kafka
type Consumer interface {
	// Consume returns a channel of messages.
	Consume(ctx context.Context) (<-chan Message, <-chan error)

	// Commit commits the offset for a specific message
	Commit(ctx context.Context, msg Message) error

	// Close gracefully shuts down the consumer
	Close() error
}

// KafkaConsumer implements the Consumer interface using kafka-go
type KafkaConsumer struct {
	reader *kafka.Reader
}

// Config holds Kafka consumer configuration
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

// NewKafkaConsumer creates a new KafkaConsumer instance
func NewKafkaConsumer(cfg Config) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &KafkaConsumer{
		reader: reader,
	}
}

// Consume starts the consumption loop
func (c *KafkaConsumer) Consume(ctx context.Context) (<-chan Message, <-chan error) {
	msgChan := make(chan Message)
	errChan := make(chan error, 1)

	go func() {
		defer close(msgChan)
		defer close(errChan)

		for {
			m, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errChan <- fmt.Errorf("failed to fetch message: %w", err)
				return
			}

			select {
			case msgChan <- Message{
				Key:    m.Key,
				Value:  m.Value,
				Offset: m.Offset,
				Topic:  m.Topic,
				Raw:    m,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return msgChan, errChan
}

// Commit commits the offset for a message
func (c *KafkaConsumer) Commit(ctx context.Context, msg Message) error {
	return c.reader.CommitMessages(ctx, msg.Raw)
}

// Close gracefully shuts down the consumer
func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
