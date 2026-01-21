package changestream

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeEvent represents a MongoDB change stream event
type ChangeEvent struct {
	ID            string                 `json:"id"`
	OperationType string                 `json:"operation_type"`
	Namespace     Namespace              `json:"namespace"`
	DocumentKey   map[string]interface{} `json:"document_key"`
	FullDocument  map[string]interface{} `json:"full_document"`
	ClusterTime   primitive.Timestamp    `json:"cluster_time"`
	ResumeToken   bson.Raw               `json:"-"`
}

type Namespace struct {
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

// Watcher defines the interface for monitoring MongoDB change streams
type Watcher interface {
	// Watch starts monitoring the change stream from the given resume token.
	// Returns a channel of change events and an error channel.
	Watch(ctx context.Context, resumeToken bson.Raw) (<-chan ChangeEvent, <-chan error)

	// Close gracefully shuts down the watcher
	Close() error
}

// MongoWatcher implements the Watcher interface for MongoDB
type MongoWatcher struct {
	collection *mongo.Collection
	stream     *mongo.ChangeStream
}

// NewMongoWatcher creates a new MongoWatcher instance
func NewMongoWatcher(coll *mongo.Collection) *MongoWatcher {
	return &MongoWatcher{
		collection: coll,
	}
}

// Watch starts monitoring the change stream
func (w *MongoWatcher) Watch(ctx context.Context, resumeToken bson.Raw) (<-chan ChangeEvent, <-chan error) {
	eventChan := make(chan ChangeEvent)
	errChan := make(chan error, 1)

	go func() {
		defer close(eventChan)
		defer close(errChan)

		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
		if resumeToken != nil {
			opts.SetResumeAfter(resumeToken)
		}

		stream, err := w.collection.Watch(ctx, mongo.Pipeline{}, opts)
		if err != nil {
			errChan <- fmt.Errorf("failed to open change stream: %w", err)
			return
		}
		w.stream = stream
		defer stream.Close(ctx)

		for stream.Next(ctx) {
			var rawEvent bson.Raw
			if err := stream.Decode(&rawEvent); err != nil {
				errChan <- fmt.Errorf("failed to decode change event: %w", err)
				continue
			}

			event, err := w.parseEvent(rawEvent)
			if err != nil {
				errChan <- fmt.Errorf("failed to parse change event: %w", err)
				continue
			}

			// Attach the actual resume token from the stream
			event.ResumeToken = stream.ResumeToken()

			select {
			case eventChan <- event:
			case <-ctx.Done():
				return
			}
		}

		if err := stream.Err(); err != nil {
			errChan <- fmt.Errorf("change stream error: %w", err)
		}
	}()

	return eventChan, errChan
}

func (w *MongoWatcher) parseEvent(raw bson.Raw) (ChangeEvent, error) {
	var event struct {
		ID            interface{}            `bson:"_id"`
		OperationType string                 `bson:"operationType"`
		FullDocument  map[string]interface{} `bson:"fullDocument"`
		DocumentKey   map[string]interface{} `bson:"documentKey"`
		ClusterTime   primitive.Timestamp    `bson:"clusterTime"`
		Namespace     struct {
			DB   string `bson:"db"`
			Coll string `bson:"coll"`
		} `bson:"ns"`
	}

	if err := bson.Unmarshal(raw, &event); err != nil {
		return ChangeEvent{}, err
	}

	var eventID string
	switch v := event.ID.(type) {
	case primitive.ObjectID:
		eventID = v.Hex()
	default:
		eventID = fmt.Sprintf("%v", v)
	}

	return ChangeEvent{
		ID:            eventID,
		OperationType: event.OperationType,
		FullDocument:  event.FullDocument,
		DocumentKey:   event.DocumentKey,
		ClusterTime:   event.ClusterTime,
		Namespace: Namespace{
			Database:   event.Namespace.DB,
			Collection: event.Namespace.Coll,
		},
	}, nil
}

// Close gracefully shuts down the watcher
func (w *MongoWatcher) Close() error {
	if w.stream != nil {
		return w.stream.Close(context.Background())
	}
	return nil
}
