package writer

import (
	"context"
	"fmt"
	"time"

	"watcher/pkg/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// PostgresWriter defines the interface for writing batches to PostgreSQL
type PostgresWriter interface {
	// WriteBatch writes a batch of records to PostgreSQL.
	// Uses COPY protocol for large batches, prepared statements for small batches.
	WriteBatch(ctx context.Context, records []PlayerRecord) error

	// Close closes the database connection pool
	Close() error
}

// PGWriter implements PostgresWriter using pgxpool
type PGWriter struct {
	pool   *pgxpool.Pool
	logger *logger.Logger
}

// PostgresConfig holds database connection settings
type PostgresConfig struct {
	URI      string
	MinConns int32
	MaxConns int32
}

// NewPostgresWriter creates a new PGWriter instance
func NewPostgresWriter(ctx context.Context, cfg PostgresConfig, l *logger.Logger) (*PGWriter, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PGWriter{pool: pool, logger: l}, nil
}

// WriteBatch writes the records using the best available protocol
func (w *PGWriter) WriteBatch(ctx context.Context, records []PlayerRecord) error {
	if len(records) == 0 {
		return nil
	}

	if len(records) >= 100 {
		return w.writeBatchCopy(ctx, records)
	}
	return w.writeBatchInsert(ctx, records)
}

// writeBatchInsert uses standard INSERT with UPSERT logic for smaller batches
func (w *PGWriter) writeBatchInsert(ctx context.Context, records []PlayerRecord) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	const query = `
		INSERT INTO players (id, username, level, score, created_at, updated_at, cdc_operation, cdc_timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			username = EXCLUDED.username,
			level = EXCLUDED.level,
			score = EXCLUDED.score,
			updated_at = EXCLUDED.updated_at,
			cdc_operation = EXCLUDED.cdc_operation,
			cdc_timestamp = EXCLUDED.cdc_timestamp
		RETURNING (xmax = 0) AS inserted
	`
	for _, r := range records {
		var inserted bool
		err := tx.QueryRow(ctx, query, r.ID, r.Username, r.Level, r.Score, r.CreatedAt, r.UpdatedAt, r.CDCOperation, r.CDCTimestamp).Scan(&inserted)
		if err != nil {
			return err
		}

		status := "updated"
		if inserted {
			status = "inserted"
		}
		w.logger.Debug("upsert complete", zap.String("id", r.ID), zap.String("status", status))
	}
	return tx.Commit(ctx)
}

// writeBatchCopy uses the COPY protocol for faster initial ingest or large updates.
func (w *PGWriter) writeBatchCopy(ctx context.Context, records []PlayerRecord) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Use a unique temp table name per session or trans
	_, err = tx.Exec(ctx, "CREATE TEMP TABLE players_temp (LIKE players) ON COMMIT DROP")
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	rows := make([][]interface{}, len(records))
	for i, r := range records {
		rows[i] = []interface{}{r.ID, r.Username, r.Level, r.Score, r.CreatedAt, r.UpdatedAt, r.CDCOperation, r.CDCTimestamp}
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"players_temp"},
		[]string{"id", "username", "level", "score", "created_at", "updated_at", "cdc_operation", "cdc_timestamp"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy from failed: %w", err)
	}

	const upsertQuery = `
		INSERT INTO players SELECT * FROM players_temp
		ON CONFLICT (id) DO UPDATE SET
			username = EXCLUDED.username,
			level = EXCLUDED.level,
			score = EXCLUDED.score,
			updated_at = EXCLUDED.updated_at,
			cdc_operation = EXCLUDED.cdc_operation,
			cdc_timestamp = EXCLUDED.cdc_timestamp
	`
	_, err = tx.Exec(ctx, upsertQuery)
	if err != nil {
		return fmt.Errorf("upsert from temp table failed: %w", err)
	}

	return tx.Commit(ctx)
}

// Close closes the pool
func (w *PGWriter) Close() error {
	w.pool.Close()
	return nil
}

// Exported for testing protocol selection
func (w *PGWriter) ShouldUseCopy(records []PlayerRecord) bool {
	return len(records) >= 100
}
