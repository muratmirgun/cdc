package server

import (
	"context"
	"net/http"

	"watcher/pkg/logger"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server handles health checks and metrics
type Server struct {
	httpServer *http.Server
	logger     *logger.Logger
}

// New creates a new observability server
func New(addr string, l *logger.Logger) *Server {
	mux := http.NewServeMux()

	s := &Server{
		logger: l,
	}

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.Handle("/metrics", promhttp.Handler())

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// In a real app, check DB connections, etc.
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready"))
}

// Start runs the HTTP server
func (s *Server) Start() error {
	s.logger.Info("starting observability server", zap.String("addr", s.httpServer.Addr))
	if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown stops the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
