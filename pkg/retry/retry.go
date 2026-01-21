package retry

import (
	"context"
	"math"
	"time"
)

// RetryableFunc is a function that can be retried
type RetryableFunc func() error

// ErrorClassifier determines if an error is retryable
type ErrorClassifier func(error) bool

// RetryOptions defines the configuration for retries
type RetryOptions struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Classifier      ErrorClassifier
}

// DefaultOptions returns a set of sensible default retry options
func DefaultOptions() RetryOptions {
	return RetryOptions{
		MaxAttempts:     5,
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
		Classifier: func(err error) bool {
			return true // Default to retry all errors
		},
	}
}

// Do executes the function with exponential backoff retries
func Do(ctx context.Context, fn RetryableFunc, opts RetryOptions) error {
	var lastErr error
	interval := opts.InitialInterval

	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if opts.Classifier != nil && !opts.Classifier(err) {
			return err
		}

		// Don't wait on last attempt
		if attempt == opts.MaxAttempts {
			break
		}

		// Wait with backoff or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
			// Calculate next interval
			nextInterval := float64(interval) * opts.Multiplier
			if nextInterval > float64(opts.MaxInterval) {
				interval = opts.MaxInterval
			} else {
				interval = time.Duration(nextInterval)
			}
		}
	}

	return lastErr
}

// CalculateBackoff returns the interval for a specific attempt number
func CalculateBackoff(attempt int, opts RetryOptions) time.Duration {
	if attempt <= 1 {
		return opts.InitialInterval
	}

	interval := float64(opts.InitialInterval) * math.Pow(opts.Multiplier, float64(attempt-1))
	if interval > float64(opts.MaxInterval) {
		return opts.MaxInterval
	}
	return time.Duration(interval)
}
