package changestream

import (
	"reflect"
	"testing"

	"github.com/goccy/go-json"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSerializationRoundTrip(t *testing.T) {
	// Property 9: Message Serialization Round-Trip
	// Validates: Requirements 3.5, 4.3
	properties := gopter.NewProperties(nil)

	properties.Property("JSON serialization round-trip preserves data", prop.ForAll(
		func(id, opType, db, coll string, doc map[string]string) bool {
			event := ChangeEvent{
				ID:            id,
				OperationType: opType,
				Namespace: Namespace{
					Database:   db,
					Collection: coll,
				},
				FullDocument: make(map[string]interface{}),
				ClusterTime:  primitive.Timestamp{T: 100, I: 1},
			}
			for k, v := range doc {
				event.FullDocument[k] = v
			}

			data, err := json.Marshal(event)
			if err != nil {
				return false
			}

			var decoded ChangeEvent
			if err := json.Unmarshal(data, &decoded); err != nil {
				return false
			}

			// Check primary fields
			// Note: document_key is omitted here for simplicity in generator but same logic applies
			return decoded.ID == event.ID &&
				decoded.OperationType == event.OperationType &&
				decoded.Namespace.Database == event.Namespace.Database &&
				decoded.Namespace.Collection == event.Namespace.Collection &&
				decoded.ClusterTime == event.ClusterTime &&
				reflect.DeepEqual(decoded.FullDocument, event.FullDocument)
		},
		gen.Identifier(),
		gen.OneConstOf("insert", "update", "replace"),
		gen.Identifier(),
		gen.Identifier(),
		gen.MapOf(gen.Identifier(), gen.AnyString()),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}
