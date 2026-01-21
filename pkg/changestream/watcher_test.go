package changestream

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestParseEventProperty(t *testing.T) {
	// Property 2: Change Event Data Extraction Correctness
	// Validates: Requirements 1.3
	properties := gopter.NewProperties(nil)

	properties.Property("parseEvent correctly extracts data from BSON", prop.ForAll(
		func(opType string, dbName, collName string) bool {
			id := primitive.NewObjectID()
			clusterTime := primitive.Timestamp{T: 12345, I: 1}

			rawEvent, _ := bson.Marshal(map[string]interface{}{
				"_id":           id,
				"operationType": opType,
				"ns": map[string]string{
					"db":   dbName,
					"coll": collName,
				},
				"clusterTime": clusterTime,
				"fullDocument": map[string]string{
					"test": "data",
				},
				"documentKey": map[string]interface{}{
					"_id": id,
				},
			})

			w := &MongoWatcher{}
			event, err := w.parseEvent(bson.Raw(rawEvent))
			if err != nil {
				return false
			}

			return event.ID == id.Hex() &&
				event.OperationType == opType &&
				event.Namespace.Database == dbName &&
				event.Namespace.Collection == collName &&
				event.ClusterTime == clusterTime &&
				event.FullDocument["test"] == "data"
		},
		gen.OneConstOf("insert", "update", "replace", "delete"),
		gen.Identifier(),
		gen.Identifier(),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}

func TestParseEventError(t *testing.T) {
	w := &MongoWatcher{}
	_, err := w.parseEvent(bson.Raw{0x00, 0x01}) // Invalid BSON
	if err == nil {
		t.Error("expected error for invalid BSON")
	}
}

func BenchmarkParseEvent(b *testing.B) {
	id := primitive.NewObjectID()
	clusterTime := primitive.Timestamp{T: 12345, I: 1}

	rawEvent, _ := bson.Marshal(map[string]interface{}{
		"_id":           id,
		"operationType": "insert",
		"ns": map[string]string{
			"db":   "test_db",
			"coll": "test_coll",
		},
		"clusterTime": clusterTime,
		"fullDocument": map[string]string{
			"test": "data",
		},
		"documentKey": map[string]interface{}{
			"_id": id,
		},
	})

	w := &MongoWatcher{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := w.parseEvent(bson.Raw(rawEvent))
		if err != nil {
			b.Fatal(err)
		}
	}
}
