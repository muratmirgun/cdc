package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"watcher/internal/syncer"
	"watcher/pkg/config"
	"watcher/pkg/consumer"
	"watcher/pkg/logger"
	"watcher/pkg/server"
	"watcher/pkg/worker"
	"watcher/pkg/writer"

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
		ServiceName: "syncer",
	})
	if err != nil {
		fmt.Printf("failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer l.Sync()

	l.Info("syncer service initializing", zap.String("env", cfg.Environment))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 3. Initialize PostgreSQL
	pgWriter, err := writer.NewPostgresWriter(ctx, writer.PostgresConfig{
		URI:      cfg.Postgres.URI,
		MinConns: int32(cfg.Postgres.MinConns),
		MaxConns: int32(cfg.Postgres.MaxConns),
	}, l)
	if err != nil {
		l.Error("failed to connect to postgres", err)
		os.Exit(1)
	}
	defer pgWriter.Close()

	// 4. Initialize Consumer
	kafkaConsumer := consumer.NewKafkaConsumer(consumer.Config{
		Brokers: cfg.Kafka.Brokers,
		Topic:   cfg.Kafka.Topic,
		GroupID: "syncer-group",
	})
	defer kafkaConsumer.Close()

	// 5. Initialize Worker Pool
	workerPool := worker.NewWorkerPool(
		l,
		pgWriter,
		kafkaConsumer,
		cfg.Syncer.WorkerCount,
		cfg.Syncer.BatchSize,
		cfg.Syncer.FlushInterval,
	)

	// 6. Create service
	svc := syncer.NewService(l, kafkaConsumer, workerPool)

	// 7. Start observability server
	obsServer := server.New(":8081", l)
	go func() {
		if err := obsServer.Start(); err != nil {
			l.Error("observability server failed", err)
		}
	}()

	// 8. Start service
	l.Info("syncer service starting")
	if err := svc.Start(ctx); err != nil {
		if err == context.Canceled {
			l.Info("syncer service stopping")
		} else {
			l.Error("syncer service failed", err)
		}
	}

	// Clean up observability server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	obsServer.Shutdown(shutdownCtx)
}
