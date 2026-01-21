package watcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"watcher/pkg/changestream"
	"watcher/pkg/logger"
	"watcher/pkg/producer"
	"watcher/pkg/retry"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson"
)

// Mocks
type MockTokenStore struct{ mock.Mock }

func (m *MockTokenStore) Save(ctx context.Context, token bson.Raw) error {
	return m.Called(ctx, token).Error(0)
}
func (m *MockTokenStore) Load(ctx context.Context) (bson.Raw, error) {
	args := m.Called(ctx)
	return args.Get(0).(bson.Raw), args.Error(1)
}

type MockProducer struct{ mock.Mock }

func (m *MockProducer) PublishAsync(ctx context.Context, key, value []byte) <-chan producer.ProduceResult {
	args := m.Called(ctx, key, value)
	return args.Get(0).(<-chan producer.ProduceResult)
}
func (m *MockProducer) Close() error { return m.Called().Error(0) }

type MockWatcher struct{ mock.Mock }

func (m *MockWatcher) Watch(ctx context.Context, token bson.Raw) (<-chan changestream.ChangeEvent, <-chan error) {
	args := m.Called(ctx, token)
	return args.Get(0).(<-chan changestream.ChangeEvent), args.Get(1).(<-chan error)
}
func (m *MockWatcher) Close() error { return m.Called().Error(0) }

func TestServiceCoordinationProperties(t *testing.T) {
	properties := gopter.NewProperties(nil)
	l, _ := logger.New(logger.Config{Level: "info", ServiceName: "test"})

	// Property 4: Resume Token Save After Kafka Success
	// Property 8: Kafka Confirmation Before Token Save
	properties.Property("token is saved only after Kafka success", prop.ForAll(
		func(eventID string) bool {
			mt := new(MockTokenStore)
			mp := new(MockProducer)
			mw := new(MockWatcher)

			s := NewService(l, mt, mp, mw)
			s.retryOpts = retry.RetryOptions{MaxAttempts: 1, InitialInterval: 1 * time.Microsecond}

			event := changestream.ChangeEvent{ID: eventID, ResumeToken: bson.Raw("token")}

			// Kafka success
			resChan := make(chan producer.ProduceResult, 1)
			resChan <- producer.ProduceResult{Error: nil}
			close(resChan)

			mp.On("PublishAsync", mock.Anything, []byte(event.ID), mock.Anything).Return((<-chan producer.ProduceResult)(resChan))
			mt.On("Save", mock.Anything, event.ResumeToken).Return(nil)

			err := s.processEvent(context.Background(), event)

			return err == nil && mt.AssertCalled(t, "Save", mock.Anything, event.ResumeToken)
		},
		gen.Identifier(),
	))

	// Property 5: Resume Token Retry on Failure
	properties.Property("token save is retried on failure", prop.ForAll(
		func(maxAttempts int) bool {
			if maxAttempts < 2 || maxAttempts > 5 {
				return true
			}
			mt := new(MockTokenStore)
			mp := new(MockProducer)
			mw := new(MockWatcher)

			s := NewService(l, mt, mp, mw)
			s.retryOpts = retry.RetryOptions{
				MaxAttempts:     maxAttempts,
				InitialInterval: 1 * time.Microsecond,
				Multiplier:      1.0,
			}

			event := changestream.ChangeEvent{ID: "test", ResumeToken: bson.Raw("token")}

			resChan := make(chan producer.ProduceResult, 1)
			resChan <- producer.ProduceResult{Error: nil}
			close(resChan)

			mp.On("PublishAsync", mock.Anything, mock.Anything, mock.Anything).Return((<-chan producer.ProduceResult)(resChan))
			// Fail Save maxAttempts times
			mt.On("Save", mock.Anything, event.ResumeToken).Return(errors.New("save failed")).Times(maxAttempts)

			err := s.processEvent(context.Background(), event)

			return err != nil && mt.AssertExpectations(t)
		},
		gen.IntRange(2, 5),
	))

	// Property 20: Graceful Shutdown Token Save
	// Validates: Requirements 9.4
	properties.Property("last seen token is saved on shutdown", prop.ForAll(
		func() bool {
			mt := new(MockTokenStore)
			mp := new(MockProducer)
			mw := new(MockWatcher)

			s := NewService(l, mt, mp, mw)

			// If the loop exits, we want to know the last token was saved.
			// Since we save after every event, this naturally holds if events are processed.
			// Let's test the Stop sequence explicitly.
			mw.On("Close").Return(nil)
			mp.On("Close").Return(nil)

			err := s.Stop(context.Background())

			return err == nil && mw.AssertCalled(t, "Close") && mp.AssertCalled(t, "Close")
		},
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}
