package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"shardscale/internal/api"
	"shardscale/internal/metrics"
)

type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
	metrics    *metrics.Metrics
}

type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (sw *statusWriter) WriteHeader(statusCode int) {
	sw.statusCode = statusCode
	sw.ResponseWriter.WriteHeader(statusCode)
}

func New(addr string, logger *slog.Logger, handlers *api.Handlers, m *metrics.Metrics) *Server {
	mux := http.NewServeMux()
	handlers.Register(mux)

	// Request logging is composed here so future routing/rebalance middleware can
	// be inserted without changing endpoint business logic.
	root := requestLoggingMiddleware(logger, m, mux)

	httpSrv := &http.Server{
		Addr:         addr,
		Handler:      root,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{httpServer: httpSrv, logger: logger, metrics: m}
}

func (s *Server) Run() error {
	s.logger.Info("http server listening", slog.String("addr", s.httpServer.Addr))
	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func requestLoggingMiddleware(logger *slog.Logger, m *metrics.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(sw, r)

		m.TotalRequests.Add(1)
		if sw.statusCode >= http.StatusBadRequest {
			m.FailedRequests.Add(1)
		}

		logger.Info("request complete",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", sw.statusCode),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
			slog.String("remote_addr", r.RemoteAddr),
		)
	})
}
