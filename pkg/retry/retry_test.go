package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
)

func TestRetryProperties(t *testing.T) {
	properties := gopter.NewProperties(nil)

	// Property 24: Exponential Backoff Retry Pattern
	// Validates: Requirements 1.4, 3.4, 12.1, 12.4
	properties.Property("exponential backoff calculates intervals correctly", prop.ForAll(
		func(initialNs, maxNs int64, multiplier float64, attempt int) bool {
			initial := time.Duration(initialNs)
			max := time.Duration(maxNs)

			// Constrain inputs for sanity
			if attempt < 1 || attempt > 10 {
				return true
			}
			opts := RetryOptions{
				InitialInterval: initial,
				Multiplier:      multiplier,
				MaxInterval:     max,
			}
			backoff := CalculateBackoff(attempt, opts)

			// Basic sanity checks
			if backoff > max {
				return false
			}
			if attempt == 1 && backoff != initial {
				return false
			}
			return true
		},
		gen.Int64Range(int64(10*time.Millisecond), int64(100*time.Millisecond)), // initialNs
		gen.Int64Range(int64(1*time.Second), int64(5*time.Second)),              // maxNs
		gen.Float64Range(1.1, 3.0),                                              // multiplier
		gen.IntRange(1, 10),                                                     // attempt
	))

	// Property 25: Maximum Retry Count Enforcement
	// Validates: Requirements 12.2
	properties.Property("retry does not exceed max attempts", prop.ForAll(
		func(maxAttempts int) bool {
			if maxAttempts < 1 || maxAttempts > 10 {
				return true
			}

			count := 0
			fn := func() error {
				count++
				return errors.New("transient error")
			}

			opts := DefaultOptions()
			opts.MaxAttempts = maxAttempts
			opts.InitialInterval = 1 * time.Microsecond // fast for testing
			opts.MaxInterval = 10 * time.Microsecond

			_ = Do(context.Background(), fn, opts)

			return count == maxAttempts
		},
		gen.IntRange(1, 10),
	))

	// Property 26: Error Type Classification
	// Validates: Requirements 12.5
	properties.Property("non-retryable errors stop retry loop immediately", prop.ForAll(
		func(failAtAttempt int) bool {
			if failAtAttempt < 1 || failAtAttempt > 5 {
				return true
			}

			count := 0
			fn := func() error {
				count++
				if count == failAtAttempt {
					return errors.New("fatal error")
				}
				return errors.New("retryable error")
			}

			opts := DefaultOptions()
			opts.MaxAttempts = 10
			opts.InitialInterval = 1 * time.Microsecond
			opts.MaxInterval = 10 * time.Microsecond
			opts.Classifier = func(err error) bool {
				return err.Error() == "retryable error"
			}

			err := Do(context.Background(), fn, opts)

			return count == failAtAttempt && err.Error() == "fatal error"
		},
		gen.IntRange(1, 5),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

func TestRetrySuccess(t *testing.T) {
	count := 0
	fn := func() error {
		count++
		if count < 3 {
			return errors.New("not yet")
		}
		return nil
	}

	opts := DefaultOptions()
	opts.InitialInterval = 1 * time.Millisecond

	err := Do(context.Background(), fn, opts)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestRetryContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fn := func() error {
		return errors.New("waiting")
	}

	opts := DefaultOptions()
	opts.InitialInterval = 100 * time.Millisecond

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := Do(ctx, fn, opts)
	assert.ErrorIs(t, err, context.Canceled)
}
