package token

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestTokenStoreProperties(t *testing.T) {
	properties := gopter.NewProperties(nil)

	// Setup Redis mock
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Setup temporary directory for file storage
	tmpDir, err := os.MkdirTemp("", "tokenstore-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Property 3: Resume Token Persistence Round-Trip
	// Validates: Requirements 2.2
	properties.Property("FileTokenStore persists and loads tokens correctly", prop.ForAll(
		func(tokenData []byte) bool {
			token := bson.Raw(tokenData)
			path := filepath.Join(tmpDir, "token.bin")
			// Clean up file if exists
			os.Remove(path)

			s := NewFileTokenStore(path)
			if err := s.Save(context.Background(), token); err != nil {
				return false
			}

			loaded, err := s.Load(context.Background())
			if err != nil {
				return false
			}

			return string(loaded) == string(token)
		},
		gen.SliceOf(gen.UInt8()),
	))

	properties.Property("RedisTokenStore persists and loads tokens correctly", prop.ForAll(
		func(tokenData []byte, key string) bool {
			if key == "" {
				return true
			}
			token := bson.Raw(tokenData)
			s := NewRedisTokenStore(redisClient, key)

			if err := s.Save(context.Background(), token); err != nil {
				return false
			}

			loaded, err := s.Load(context.Background())
			if err != nil {
				return false
			}

			return string(loaded) == string(token)
		},
		gen.SliceOf(gen.UInt8()),
		gen.Identifier(),
	))

	// Property 6: Resume Token Storage Backend Equivalence
	// Validates: Requirements 2.5
	properties.Property("File and Redis backends are equivalent", prop.ForAll(
		func(tokenData []byte) bool {
			token := bson.Raw(tokenData)
			key := "equiv-test-key"
			path := filepath.Join(tmpDir, "equiv-test.bin")

			fileStore := NewFileTokenStore(path)
			redisStore := NewRedisTokenStore(redisClient, key)

			if err := fileStore.Save(context.Background(), token); err != nil {
				return false
			}
			if err := redisStore.Save(context.Background(), token); err != nil {
				return false
			}

			fileLoaded, err := fileStore.Load(context.Background())
			if err != nil {
				return false
			}
			redisLoaded, err := redisStore.Load(context.Background())
			if err != nil {
				return false
			}

			return string(fileLoaded) == string(redisLoaded)
		},
		gen.SliceOf(gen.UInt8()),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}
