package parser

import (
	"testing"

	"github.com/goccy/go-json"

	"watcher/pkg/changestream"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestParsePlayerRecordProperty(t *testing.T) {
	properties := gopter.NewProperties(nil)

	// Property 16: Message Deserialization Correctness
	properties.Property("parsed record matches event data", prop.ForAll(
		func(id, username string, level int, score int64) bool {
			event := changestream.ChangeEvent{
				ID:            id,
				OperationType: "insert",
				FullDocument: map[string]interface{}{
					"username": username,
					"level":    level,
					"score":    score,
				},
				ClusterTime: primitive.Timestamp{T: 100, I: 1},
			}

			data, _ := json.Marshal(event)
			record, err := ParsePlayerRecord(data)
			if err != nil {
				return false
			}

			return record.ID == id &&
				record.Username == username &&
				record.Level == level &&
				record.Score == score
		},
		gen.Identifier(),
		gen.AnyString(),
		gen.IntRange(1, 100),
		gen.Int64(),
	))

	// Property 19: Error Handling for Malformed Messages
	properties.Property("invalid JSON returns error", prop.ForAll(
		func(data string) bool {
			// If data happens to be valid JSON, ParsePlayerRecord might succeed or return a validation error.
			// But for purely random strings, it should mostly return unmarshal error.
			_, err := ParsePlayerRecord([]byte(data))
			if json.Valid([]byte(data)) {
				return true // If it was valid JSON, we don't care about the error for this test
			}
			return err != nil
		},
		gen.AnyString(),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

func TestParsePlayerRecordValidation(t *testing.T) {
	// Missing ID
	data, _ := json.Marshal(changestream.ChangeEvent{OperationType: "insert"})
	_, err := ParsePlayerRecord(data)
	assert.Error(t, err)

	// Missing OpType
	data, _ = json.Marshal(changestream.ChangeEvent{ID: "test"})
	_, err = ParsePlayerRecord(data)
	assert.Error(t, err)
}

func TestParsePlayerRecordDelete(t *testing.T) {
	// Delete operation uses DocumentKey
	data, _ := json.Marshal(changestream.ChangeEvent{
		ID:            "del-1",
		OperationType: "delete",
		DocumentKey: map[string]interface{}{
			"username": "deleted-user",
		},
	})
	record, err := ParsePlayerRecord(data)
	assert.NoError(t, err)
	assert.Equal(t, "del-1", record.ID)
	assert.Equal(t, "deleted-user", record.Username)
}

func BenchmarkParsePlayerRecord(b *testing.B) {
	event := changestream.ChangeEvent{
		ID:            "player-123",
		OperationType: "insert",
		FullDocument: map[string]interface{}{
			"username": "benchmark_user",
			"level":    50,
			"score":    99999,
		},
		ClusterTime: primitive.Timestamp{T: 100, I: 1},
	}
	data, _ := json.Marshal(event)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := ParsePlayerRecord(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParsePlayerRecordDelete(b *testing.B) {
	event := changestream.ChangeEvent{
		ID:            "del-123",
		OperationType: "delete",
		DocumentKey: map[string]interface{}{
			"username": "deleted-user",
		},
		ClusterTime: primitive.Timestamp{T: 100, I: 1},
	}
	data, _ := json.Marshal(event)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := ParsePlayerRecord(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
