package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"time"
	"watcher/internal/watcher"
	"watcher/pkg/changestream"
	"watcher/pkg/config"
	"watcher/pkg/logger"
	"watcher/pkg/producer"
	"watcher/pkg/server"
	"watcher/pkg/token"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func main() {
	// 1. Load config
	cfg, err := config.Load("")
	if err != nil {
		fmt.Printf("failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 2. Initialize logger
	l, err := logger.New(logger.Config{
		Level:       cfg.LogLevel,
		Environment: cfg.Environment,
		ServiceName: "watcher",
	})
	if err != nil {
		fmt.Printf("failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer l.Sync()

	l.Info("watcher service initializing", zap.String("env", cfg.Environment))

	// 3. Initialize MongoDB
	mongoCtx, mongoCancel := context.WithTimeout(context.Background(), cfg.MongoDB.ConnectTimeout)
	defer mongoCancel()
	client, err := mongo.Connect(mongoCtx, options.Client().ApplyURI(cfg.MongoDB.URI))
	if err != nil {
		l.Error("failed to connect to mongodb", err)
		os.Exit(1)
	}
	defer client.Disconnect(context.Background())

	coll := client.Database(cfg.MongoDB.Database).Collection(cfg.MongoDB.Collection)

	// 4. Initialize components
	tokenStore := token.NewFileTokenStore(cfg.Watcher.ResumeTokenPath)
	kafkaProducer := producer.NewKafkaProducer(producer.Config{
		Brokers: cfg.Kafka.Brokers,
		Topic:   cfg.Kafka.Topic,
	})
	streamWatcher := changestream.NewMongoWatcher(coll)

	// 5. Create service
	svc := watcher.NewService(l, tokenStore, kafkaProducer, streamWatcher)

	// 6. Start observability server
	obsServer := server.New(":8080", l)
	go func() {
		if err := obsServer.Start(); err != nil {
			l.Error("observability server failed", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 7. Start service
	l.Info("watcher service starting")
	if err := svc.Start(ctx); err != nil {
		if err == context.Canceled {
			l.Info("watcher service stopping")
		} else {
			l.Error("watcher service failed", err)
		}
	}

	// Clean up observability server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obsServer.Shutdown(shutdownCtx)
}
