package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"watcher/pkg/consumer"
	"watcher/pkg/logger"
	"watcher/pkg/writer"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockPGWriter struct {
	mock.Mock
}

func (m *MockPGWriter) WriteBatch(ctx context.Context, records []writer.PlayerRecord) error {
	args := m.Called(ctx, records)
	return args.Error(0)
}

func (m *MockPGWriter) Close() error {
	return m.Called().Error(0)
}

type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) Consume(ctx context.Context) (<-chan consumer.Message, <-chan error) {
	args := m.Called(ctx)
	return args.Get(0).(<-chan consumer.Message), args.Get(1).(<-chan error)
}
func (m *MockConsumer) Commit(ctx context.Context, msg consumer.Message) error {
	return m.Called(ctx, msg).Error(0)
}
func (m *MockConsumer) Close() error {
	return m.Called().Error(0)
}

func TestWorkerPoolDistribution(t *testing.T) {
	// Property 14: Worker Message Distribution
	properties := gopter.NewProperties(nil)
	l, _ := logger.New(logger.Config{Level: "error", ServiceName: "test"})

	properties.Property("all submitted messages are eventually processed", prop.ForAll(
		func(numMessages int) bool {
			if numMessages < 1 || numMessages > 100 {
				return true
			}

			mw := new(MockPGWriter)
			mc := new(MockConsumer)
			mc.On("Commit", mock.Anything, mock.Anything).Return(nil)

			// Expect WriteBatch to be called. Exact batch count depends on flush logic,
			var totalCount int
			var mu sync.Mutex

			mw.On("WriteBatch", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				records := args.Get(1).([]writer.PlayerRecord)
				mu.Lock()
				totalCount += len(records)
				mu.Unlock()
			}).Return(nil)

			p := NewWorkerPool(l, mw, mc, 2, 10, 50*time.Millisecond)
			p.Start(context.Background())

			for i := 0; i < numMessages; i++ {
				_ = p.Submit(context.Background(), Job{
					Record:  writer.PlayerRecord{ID: "test"},
					Message: consumer.Message{Key: []byte("test")},
				})
			}

			// We need to wait for workers to process and then shutdown
			// Shutdown should trigger final flush
			p.Shutdown(context.Background())

			return totalCount == numMessages
		},
		gen.IntRange(1, 100),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

func TestWorkerPoolShutdown(t *testing.T) {
	mw := new(MockPGWriter)
	mc := new(MockConsumer)
	l, _ := logger.New(logger.Config{Level: "error", ServiceName: "test"})
	p := NewWorkerPool(l, mw, mc, 1, 100, 1*time.Second)

	p.Start(context.Background())
	err := p.Shutdown(context.Background())
	assert.NoError(t, err)
}

func BenchmarkWorkerPoolSubmit(b *testing.B) {
	l, _ := logger.New(logger.Config{Level: "error", ServiceName: "test"})
	mw := new(MockPGWriter)
	mc := new(MockConsumer)

	// Suppress mock logs/overhead
	mw.On("WriteBatch", mock.Anything, mock.Anything).Return(nil)
	mc.On("Commit", mock.Anything, mock.Anything).Return(nil)

	p := NewWorkerPool(l, mw, mc, 4, 1000, 100*time.Millisecond)
	p.Start(context.Background())
	defer p.Shutdown(context.Background())

	job := Job{
		Record:  writer.PlayerRecord{ID: "test"},
		Message: consumer.Message{Key: []byte("test")},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Submit(context.Background(), job)
	}
}
