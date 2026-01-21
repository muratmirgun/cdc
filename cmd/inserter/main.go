package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/goccy/go-json"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Player struct {
	ID        string    `bson:"_id" json:"id"`
	Username  string    `bson:"username" json:"username"`
	Level     int       `bson:"level" json:"level"`
	Score     int64     `bson:"score" json:"score"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

func main() {
	addr := flag.String("addr", ":8082", "HTTP server address")
	uri := flag.String("uri", "mongodb://localhost:27017", "MongoDB URI")
	dbName := flag.String("db", "game", "Database name")
	collName := flag.String("coll", "players", "Collection name")
	flag.Parse()

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri))
	if err != nil {
		log.Fatalf("failed to connect to mongodb: %v", err)
	}
	defer client.Disconnect(context.Background())

	coll := client.Database(*dbName).Collection(*collName)

	// HTTP Handlers
	http.HandleFunc("/insert", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		p := Player{
			ID:        fmt.Sprintf("player-%d-%d", time.Now().UnixNano(), rand.Int63()),
			Username:  fmt.Sprintf("user_%d", rand.Intn(1000000)),
			Level:     rand.Intn(100) + 1,
			Score:     rand.Int63n(1000000),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := coll.InsertOne(ctx, p); err != nil {
			http.Error(w, fmt.Sprintf("failed to insert: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{Addr: *addr}

	go func() {
		fmt.Printf("Inserter server starting on %s (MongoDB: %s)\n", *addr, *uri)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
	}()

	// Signal handling for graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutting down inserter server...")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	server.Shutdown(shutdownCtx)
}
