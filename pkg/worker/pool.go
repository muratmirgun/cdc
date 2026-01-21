package worker

import (
	"context"
	"sync"
	"time"

	"watcher/pkg/consumer"
	"watcher/pkg/logger"
	"watcher/pkg/metrics"
	"watcher/pkg/writer"

	"go.uber.org/zap"
)

// Job represents a unit of work for a worker
type Job struct {
	Record  writer.PlayerRecord
	Message consumer.Message
}

// Worker represents a single processing goroutine
type Worker struct {
	id            int
	input         <-chan consumer.Message
	writer        writer.PostgresWriter
	logger        *logger.Logger
	batchSize     int
	flushInterval time.Duration
}

// WorkerPool manages a collection of workers
type WorkerPool struct {
	logger        *logger.Logger
	writer        writer.PostgresWriter
	consumer      consumer.Consumer
	numWorkers    int
	batchSize     int
	flushInterval time.Duration
	inputChan     chan Job
	wg            sync.WaitGroup
	cancel        context.CancelFunc
}

// NewWorkerPool creates a new WorkerPool instance
func NewWorkerPool(l *logger.Logger, w writer.PostgresWriter, c consumer.Consumer, numWorkers, batchSize int, flushInterval time.Duration) *WorkerPool {
	return &WorkerPool{
		logger:        l,
		writer:        w,
		consumer:      c,
		numWorkers:    numWorkers,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		inputChan:     make(chan Job, numWorkers*2), // Buffered for smooth handoff
	}
}

// Start initializes the worker goroutines
func (p *WorkerPool) Start(ctx context.Context) {
	workerCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(workerCtx, i)
	}
}

// Submit sends a message to the pool for processing
func (p *WorkerPool) Submit(ctx context.Context, job Job) error {
	select {
	case p.inputChan <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *WorkerPool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()

	p.logger.Debug("worker started", zap.Int("worker_id", id))

	buffer := writer.NewInMemoryBuffer(p.batchSize)
	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case job, ok := <-p.inputChan:
			if !ok {
				p.flush(ctx, buffer)
				return
			}

			record := writer.Record{
				Data:    job.Record,
				Message: job.Message,
			}

			if buffer.Add(record) {
				p.flush(ctx, buffer)
			}
			metrics.SyncerMessagesConsumedTotal.Inc()

		case <-ticker.C:
			if buffer.ShouldFlush(p.flushInterval) {
				p.flush(ctx, buffer)
			}

		case <-ctx.Done():
			p.flush(context.Background(), buffer) // Final flush on shutdown
			return
		}
	}
}

func (p *WorkerPool) flush(ctx context.Context, buffer *writer.InMemoryBuffer) {
	records := buffer.Flush()
	if len(records) == 0 {
		return
	}

	// Prepare data for writer
	playerRecords := make([]writer.PlayerRecord, len(records))
	for i, r := range records {
		playerRecords[i] = r.Data.(writer.PlayerRecord)
	}

	// 1. Write to PostgreSQL
	start := time.Now()
	if err := p.writer.WriteBatch(ctx, playerRecords); err != nil {
		p.logger.Error("failed to write batch", err)
		metrics.SyncerWriteErrorsTotal.Inc()
		// Error handling logic (Retry) would be integrated here in later tasks
		return
	}
	metrics.SyncerUpsertLatency.Observe(time.Since(start).Seconds())
	metrics.SyncerBatchWritesTotal.Inc()

	// 2. Commit offsets only after success (Requirement 4.5)
	for _, r := range records {
		if err := p.consumer.Commit(ctx, r.Message); err != nil {
			p.logger.Error("failed to commit offset", err, zap.Int64("offset", r.Message.Offset))
		}
	}
}

// Shutdown stops all workers and waits for them to finish
func (p *WorkerPool) Shutdown(ctx context.Context) error {
	close(p.inputChan)

	// Wait for workers to finish current work and flush
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
