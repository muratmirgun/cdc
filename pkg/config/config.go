package config

import (
	"errors"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// AppConfig holds the complete configuration for the application
type AppConfig struct {
	Environment string         `mapstructure:"environment"`
	LogLevel    string         `mapstructure:"log_level"`
	ServiceName string         `mapstructure:"service_name"`
	MongoDB     MongoConfig    `mapstructure:"mongodb"`
	Kafka       KafkaConfig    `mapstructure:"kafka"`
	Postgres    PostgresConfig `mapstructure:"postgres"`
	Watcher     WatcherConfig  `mapstructure:"watcher"`
	Syncer      SyncerConfig   `mapstructure:"syncer"`
}

type MongoConfig struct {
	URI            string        `mapstructure:"uri"`
	Database       string        `mapstructure:"database"`
	Collection     string        `mapstructure:"collection"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	GroupID string   `mapstructure:"group_id"`
}

type PostgresConfig struct {
	URI             string        `mapstructure:"uri"`
	MaxConns        int           `mapstructure:"max_conns"`
	MinConns        int           `mapstructure:"min_conns"`
	MaxConnLifetime time.Duration `mapstructure:"max_conn_lifetime"`
}

type WatcherConfig struct {
	ResumeTokenPath string `mapstructure:"resume_token_path"`
}

type SyncerConfig struct {
	BatchSize     int           `mapstructure:"batch_size"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	WorkerCount   int           `mapstructure:"worker_count"`
}

// Load loads configuration from file and environment variables
func Load(path string) (*AppConfig, error) {
	v := viper.New()

	// Default values
	v.SetDefault("environment", "development")
	v.SetDefault("log_level", "info")
	v.SetDefault("mongodb.connect_timeout", 10*time.Second)
	v.SetDefault("postgres.max_conns", 50)
	v.SetDefault("postgres.min_conns", 10)
	v.SetDefault("postgres.uri", "")
	v.SetDefault("watcher.resume_token_path", "resume_token.bin")
	v.SetDefault("syncer.batch_size", 1000)
	v.SetDefault("syncer.flush_interval", 500*time.Millisecond)
	v.SetDefault("syncer.worker_count", 10)

	// Environment variables
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Config file
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	// Bind environment variables explicitly for nested structs to ensure Unmarshal picks them up
	v.BindEnv("service_name", "SERVICE_NAME")
	v.BindEnv("environment", "ENVIRONMENT")
	v.BindEnv("log_level", "LOG_LEVEL")
	v.BindEnv("mongodb.uri", "MONGODB_URI")
	v.BindEnv("mongodb.database", "MONGODB_DATABASE")
	v.BindEnv("mongodb.collection", "MONGODB_COLLECTION")
	v.BindEnv("kafka.brokers", "KAFKA_BROKERS")
	v.BindEnv("kafka.topic", "KAFKA_TOPIC")
	v.BindEnv("postgres.uri", "POSTGRES_URI")
	v.BindEnv("postgres.max_conns", "POSTGRES_MAX_CONNS")
	v.BindEnv("postgres.min_conns", "POSTGRES_MIN_CONNS")
	v.BindEnv("watcher.resume_token_path", "WATCHER_RESUME_TOKEN_PATH")
	v.BindEnv("syncer.batch_size", "SYNCER_BATCH_SIZE")
	v.BindEnv("syncer.flush_interval", "SYNCER_FLUSH_INTERVAL")
	v.BindEnv("syncer.worker_count", "SYNCER_WORKER_COUNT")

	var config AppConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	// Manual check for Kafka brokers if they came as a single string from env
	brokers := v.GetString("kafka.brokers")
	if brokers != "" && len(config.Kafka.Brokers) == 0 {
		config.Kafka.Brokers = strings.Split(brokers, ",")
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &config, nil
}

// Validate checks if the configuration is valid
func (c *AppConfig) Validate() error {
	if c.ServiceName == "" {
		return errors.New("service_name is required")
	}
	if c.MongoDB.URI == "" {
		return errors.New("mongodb.uri is required")
	}
	if c.MongoDB.Database == "" {
		return errors.New("mongodb.database is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("kafka.brokers is required")
	}
	if c.Kafka.Topic == "" {
		return errors.New("kafka.topic is required")
	}
	if c.Postgres.URI == "" {
		return errors.New("postgres.uri is required")
	}
	if c.Watcher.ResumeTokenPath == "" {
		return errors.New("watcher.resume_token_path is required")
	}
	return nil
}
