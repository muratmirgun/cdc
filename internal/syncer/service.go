package syncer

import (
	"context"
	"fmt"

	"watcher/pkg/consumer"
	"watcher/pkg/logger"
	"watcher/pkg/parser"
	"watcher/pkg/worker"

	"go.uber.org/zap"
)

// Service coordinates the Syncer components
type Service struct {
	logger     *logger.Logger
	consumer   consumer.Consumer
	workerPool *worker.WorkerPool
}

// NewService creates a new Syncer service instance
func NewService(
	l *logger.Logger,
	c consumer.Consumer,
	p *worker.WorkerPool,
) *Service {
	return &Service{
		logger:     l,
		consumer:   c,
		workerPool: p,
	}
}

// Start begins the message consumption and processing loop
func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("starting syncer service")

	// 1. Start worker pool
	s.workerPool.Start(ctx)

	// 2. Start consuming
	msgChan, errChan := s.consumer.Consume(ctx)

	// 3. Main loop
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				return nil
			}

			if err := s.handleMessage(ctx, msg); err != nil {
				s.logger.Error("failed to handle message", err, zap.Int64("offset", msg.Offset))
				// We skip malformed messages as per Requirement 9.3
			}

		case err := <-errChan:
			if err != nil {
				return fmt.Errorf("consumer error: %w", err)
			}

		case <-ctx.Done():
			return s.Shutdown(context.Background())
		}
	}
}

func (s *Service) handleMessage(ctx context.Context, msg consumer.Message) error {
	// a. Parse message
	record, err := parser.ParsePlayerRecord(msg.Value)
	if err != nil {
		// Log and skip malformed message (Requirement 9.3)
		s.logger.Warn("skipping malformed message",
			zap.Error(err),
			zap.Int64("offset", msg.Offset),
			zap.ByteString("payload", msg.Value))

		// Still commit offset for skipped message
		return s.consumer.Commit(ctx, msg)
	}

	// b. Submit to worker pool
	// Requirement 4.5: "commit Kafka offsets only after successful PostgreSQL writes"
	// The WorkerPool now receives the message and will commit it after successful flush.
	return s.workerPool.Submit(ctx, worker.Job{
		Record:  record,
		Message: msg,
	})
}

// Shutdown stops the service gracefully
func (s *Service) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down syncer service")

	errPool := s.workerPool.Shutdown(ctx)
	errCons := s.consumer.Close()

	if errPool != nil || errCons != nil {
		return fmt.Errorf("shutdown errors: pool=%v, consumer=%v", errPool, errCons)
	}
	return nil
}
