package token

import (
	"context"
	"os"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
)

// TokenStore defines the interface for persisting and loading MongoDB resume tokens
type TokenStore interface {
	// Save persists the resume token
	Save(ctx context.Context, token bson.Raw) error

	// Load retrieves the last saved resume token. Returns nil if no token exists.
	Load(ctx context.Context) (bson.Raw, error)
}

// FileTokenStore implements TokenStore using a local file
type FileTokenStore struct {
	path string
}

func NewFileTokenStore(path string) *FileTokenStore {
	return &FileTokenStore{path: path}
}

func (s *FileTokenStore) Save(ctx context.Context, token bson.Raw) error {
	return os.WriteFile(s.path, token, 0644)
}

func (s *FileTokenStore) Load(ctx context.Context) (bson.Raw, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return bson.Raw(data), nil
}

// RedisTokenStore implements TokenStore using Redis
type RedisTokenStore struct {
	client *redis.Client
	key    string
}

func NewRedisTokenStore(client *redis.Client, key string) *RedisTokenStore {
	return &RedisTokenStore{
		client: client,
		key:    key,
	}
}

func (s *RedisTokenStore) Save(ctx context.Context, token bson.Raw) error {
	return s.client.Set(ctx, s.key, []byte(token), 0).Err()
}

func (s *RedisTokenStore) Load(ctx context.Context) (bson.Raw, error) {
	data, err := s.client.Get(ctx, s.key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return bson.Raw(data), nil
}
