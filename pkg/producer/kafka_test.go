package producer

import (
	"context"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
)

// MockWriter is used to test the wrapper without a real Kafka cluster
type MockWriter struct {
	WriteFunc func(ctx context.Context, msgs ...interface{}) error
}

func TestAsyncNonBlockingProperty(t *testing.T) {
	// Property 7: Async Producer Non-Blocking Behavior
	// Validates: Requirements 3.2
	properties := gopter.NewProperties(nil)

	properties.Property("PublishAsync returns immediately", prop.ForAll(
		func(key, value []byte) bool {
			// We use a real KafkaProducer but with non-existent brokers
			// In async mode, it should still return the channel immediately.
			p := NewKafkaProducer(Config{
				Brokers: []string{"localhost:9999"},
				Topic:   "test-topic",
			})
			defer p.Close()

			start := time.Now()
			_ = p.PublishAsync(context.Background(), key, value)
			duration := time.Since(start)

			// Should be very fast (well under 10ms for just creating a goroutine and returning)
			return duration < 10*time.Millisecond
		},
		gen.SliceOf(gen.UInt8()),
		gen.SliceOf(gen.UInt8()),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

func TestPublishAsyncConfirmation(t *testing.T) {
	// This test verifies that we eventually get a result back.
	// We can't easily verify "success" without a cluster, but we can verify it doesn't hang.
	p := NewKafkaProducer(Config{
		Brokers: []string{"localhost:9999"},
		Topic:   "test-topic",
	})
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resultChan := p.PublishAsync(ctx, []byte("key"), []byte("value"))

	select {
	case res := <-resultChan:
		// Since there's no Kafka, it might error or just be in flight
		// The point is we get a result.
		_ = res
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for result")
	}
}

func TestClose(t *testing.T) {
	p := NewKafkaProducer(Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
	})
	err := p.Close()
	assert.NoError(t, err)
}
