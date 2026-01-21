package watcher

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"

	"watcher/pkg/changestream"
	"watcher/pkg/logger"
	"watcher/pkg/metrics"
	"watcher/pkg/producer"
	"watcher/pkg/retry"
	"watcher/pkg/token"

	"go.uber.org/zap"
)

// Service coordinates the Watcher components
type Service struct {
	logger     *logger.Logger
	tokenStore token.TokenStore
	producer   producer.Producer
	watcher    changestream.Watcher
	retryOpts  retry.RetryOptions
}

// NewService creates a new Watcher service instance
func NewService(
	logger *logger.Logger,
	tokenStore token.TokenStore,
	producer producer.Producer,
	watcher changestream.Watcher,
) *Service {
	return &Service{
		logger:     logger,
		tokenStore: tokenStore,
		producer:   producer,
		watcher:    watcher,
		retryOpts:  retry.DefaultOptions(),
	}
}

// Stop gracefully shuts down the service and its dependencies
func (s *Service) Stop(ctx context.Context) error {
	s.logger.Info("stopping watcher service")

	errs := []error{}
	if err := s.watcher.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close watcher: %w", err))
	}
	if err := s.producer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close producer: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during shutdown: %v", errs)
	}
	return nil
}

// Start begins the event processing loop
func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("starting watcher service")

	// Ensure cleanup on return
	defer func() {
		if err := s.Stop(context.Background()); err != nil {
			s.logger.Error("error during service stop", err)
		}
	}()

	// 1. Load resume token
	resumeToken, err := s.tokenStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load resume token: %w", err)
	}

	// 2. Start watching
	eventChan, errChan := s.watcher.Watch(ctx, resumeToken)

	// 3. Main event loop
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				return nil
			}
			if err := s.processEvent(ctx, event); err != nil {
				s.logger.Error("failed to process event", err, zap.String("event_id", event.ID))
				// Depending on requirement, we might exit or continue.
				// Requirement 12.3: "WHEN maximum retries are exceeded, THE services SHALL log a fatal error and alert operators"
				return err
			}
		case err := <-errChan:
			if err != nil {
				return fmt.Errorf("watcher error: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) processEvent(ctx context.Context, event changestream.ChangeEvent) error {
	// a. Serialize event
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	// b. Publish to Kafka with retry (Requirement 3.4)
	err = retry.Do(ctx, func() error {
		resultChan := s.producer.PublishAsync(ctx, []byte(event.ID), data)
		result := <-resultChan
		return result.Error
	}, s.retryOpts)

	if err != nil {
		metrics.WatcherPublishErrorsTotal.Inc()
		return fmt.Errorf("failed to publish event to kafka after retries: %w", err)
	}

	// c. Save resume token with retry (Requirement 2.1, 2.4, 3.3)
	err = retry.Do(ctx, func() error {
		return s.tokenStore.Save(ctx, event.ResumeToken)
	}, s.retryOpts)

	if err != nil {
		return fmt.Errorf("failed to save resume token after retries: %w", err)
	}

	metrics.WatcherEventsCapturedTotal.Inc()
	metrics.WatcherTokenSavesTotal.Inc()

	s.logger.Debug("processed event successfully", zap.String("event_id", event.ID))
	return nil
}
