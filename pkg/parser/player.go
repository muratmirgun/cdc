package parser

import (
	"fmt"
	"time"

	"github.com/goccy/go-json"

	"watcher/pkg/writer"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ParsePlayerRecord deserializes a Kafka message value into a PlayerRecord
func ParsePlayerRecord(data []byte) (writer.PlayerRecord, error) {
	// Root struct for initial parse
	var root struct {
		ID            string              `json:"id"`
		OperationType string              `json:"operation_type"`
		ClusterTime   primitive.Timestamp `json:"cluster_time"`
		FullDocument  json.RawMessage     `json:"full_document"`
		DocumentKey   json.RawMessage     `json:"document_key"`
	}

	if err := json.Unmarshal(data, &root); err != nil {
		return writer.PlayerRecord{}, fmt.Errorf("failed to unmarshal JSON envelope: %w", err)
	}

	// Validate required fields
	if root.ID == "" {
		return writer.PlayerRecord{}, fmt.Errorf("missing event ID")
	}
	if root.OperationType == "" {
		return writer.PlayerRecord{}, fmt.Errorf("missing operation type")
	}

	// Choose source for player data
	var docRaw json.RawMessage = root.FullDocument
	if root.OperationType == "delete" || len(docRaw) == 0 {
		docRaw = root.DocumentKey
	}

	// Unmarshal player data specifically to preserve precision
	var playerPart struct {
		ID        string      `json:"_id"`
		Username  string      `json:"username"`
		Level     int         `json:"level"`
		Score     int64       `json:"score"`
		CreatedAt interface{} `json:"created_at"`
		UpdatedAt interface{} `json:"updated_at"`
	}
	if len(docRaw) > 0 {
		if err := json.Unmarshal(docRaw, &playerPart); err != nil {
			return writer.PlayerRecord{}, fmt.Errorf("failed to parse document fields: %w", err)
		}
	}

	record := writer.PlayerRecord{
		ID:           playerPart.ID,
		CDCOperation: root.OperationType,
		CDCTimestamp: time.Unix(int64(root.ClusterTime.T), 0),
		Username:     playerPart.Username,
		Level:        playerPart.Level,
		Score:        playerPart.Score,
	}

	// Fallback to envelope ID if _id not in doc (shouldn't happen for MongoDB)
	if record.ID == "" {
		record.ID = root.ID
	}

	record.CreatedAt = parseTime(playerPart.CreatedAt)
	record.UpdatedAt = parseTime(playerPart.UpdatedAt)

	return record, nil
}

func parseTime(v interface{}) time.Time {
	if s, ok := v.(string); ok {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
