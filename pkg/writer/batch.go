package writer

import (
	"sync"
	"time"

	"watcher/pkg/consumer"
)

// Record represents the data extracted from a message for writing to DB
type Record struct {
	Data    interface{}
	Message consumer.Message
}

// BatchBuffer defines the interface for buffering messages before a flush
type BatchBuffer interface {
	// Add adds a record to the buffer. Returns true if buffer should be flushed.
	Add(record Record) bool

	// Flush returns all buffered records and clears the buffer
	Flush() []Record

	// Size returns the current number of buffered records
	Size() int

	// ShouldFlush checks if flush conditions are met based on time
	ShouldFlush(interval time.Duration) bool
}

// InMemoryBuffer implements BatchBuffer using a slice
type InMemoryBuffer struct {
	mu        sync.Mutex
	records   []Record
	capacity  int
	lastFlush time.Time
}

// NewInMemoryBuffer creates a new InMemoryBuffer instance
func NewInMemoryBuffer(capacity int) *InMemoryBuffer {
	return &InMemoryBuffer{
		records:   make([]Record, 0, capacity),
		capacity:  capacity,
		lastFlush: time.Now(),
	}
}

// Add adds a record to the buffer
func (b *InMemoryBuffer) Add(record Record) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, record)
	return len(b.records) >= b.capacity
}

// Flush returns the current batch and clears the buffer
func (b *InMemoryBuffer) Flush() []Record {
	b.mu.Lock()
	defer b.mu.Unlock()

	batch := b.records
	b.records = make([]Record, 0, b.capacity)
	b.lastFlush = time.Now()
	return batch
}

// Size returns the current size
func (b *InMemoryBuffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

// ShouldFlush returns true if the specified interval has passed since the last flush
func (b *InMemoryBuffer) ShouldFlush(interval time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return false
	}

	return time.Since(b.lastFlush) >= interval
}
