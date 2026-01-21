package writer

import (
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
)

func TestBatchBufferProperties(t *testing.T) {
	properties := gopter.NewProperties(nil)

	// Property 12: Message Buffering Before Flush
	// Validates: Requirements 5.1
	properties.Property("buffer adds records until capacity", prop.ForAll(
		func(cap int) bool {
			if cap < 1 || cap > 1000 {
				return true
			}
			b := NewInMemoryBuffer(cap)
			for i := 0; i < cap-1; i++ {
				shouldFlush := b.Add(Record{})
				if shouldFlush {
					return false
				}
				if b.Size() != i+1 {
					return false
				}
			}
			// One more should trigger flush
			shouldFlush := b.Add(Record{})
			return shouldFlush && b.Size() == cap
		},
		gen.IntRange(1, 1000),
	))

	// Property 13: Buffer Clear After Flush
	// Validates: Requirements 5.4
	properties.Property("buffer is cleared after flush", prop.ForAll(
		func(count int) bool {
			if count < 0 || count > 500 {
				return true
			}
			b := NewInMemoryBuffer(1000)
			for i := 0; i < count; i++ {
				b.Add(Record{})
			}

			batch := b.Flush()
			return len(batch) == count && b.Size() == 0
		},
		gen.IntRange(0, 500),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

func TestBufferTimeFlush(t *testing.T) {
	b := NewInMemoryBuffer(100)

	// Initially should not flush
	assert.False(t, b.ShouldFlush(100*time.Millisecond))

	// Add one
	b.Add(Record{})

	// Should not flush immediately
	assert.False(t, b.ShouldFlush(100*time.Millisecond))

	// Wait
	time.Sleep(110 * time.Millisecond)

	// Now should flush
	assert.True(t, b.ShouldFlush(100*time.Millisecond))
}
