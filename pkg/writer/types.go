package writer

import "time"

// PlayerRecord represents the player data stored in PostgreSQL
type PlayerRecord struct {
	ID           string    `db:"id"`
	Username     string    `db:"username"`
	Level        int       `db:"level"`
	Score        int64     `db:"score"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
	CDCOperation string    `db:"cdc_operation"`
	CDCTimestamp time.Time `db:"cdc_timestamp"`
}
