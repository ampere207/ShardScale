package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"shardscale/internal/metrics"
	"shardscale/internal/router"
	"shardscale/internal/store"
)

type putRequest struct {
	Value string `json:"value"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type statusResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type Handlers struct {
	router  *router.Router
	metrics *metrics.Metrics
	logger  *slog.Logger
}

func NewHandlers(r *router.Router, m *metrics.Metrics, logger *slog.Logger) *Handlers {
	return &Handlers{router: r, metrics: m, logger: logger}
}

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("/kv/", h.handleKV)
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/health", h.handleHealth)
}

func (h *Handlers) handleKV(w http.ResponseWriter, r *http.Request) {
	key, ok := parseKey(r.URL.Path)
	if !ok {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "missing or invalid key"})
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodGet:
		h.handleGet(w, r, key)
	default:
		w.Header().Set("Allow", "PUT, GET")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
	}
}

func (h *Handlers) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	// Prevent forwarding loops: if request already forwarded but still not owner, error.
	if r.Header.Get("X-ShardScale-Forwarded") == "true" {
		owner := h.router.KeyOwner(key)
		if owner != h.router.SelfID {
			h.logger.Warn("forwarding loop detected",
				slog.String("key", key),
				slog.String("owner", owner),
				slog.String("self_id", h.router.SelfID),
			)
			h.metrics.FailedRequests.Add(1)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "forwarding loop detected"})
			return
		}
	}

	h.metrics.TotalWrites.Add(1)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req putRequest
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "request body must contain a single JSON object"})
		return
	}

	if req.Value == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "value must be non-empty"})
		return
	}

	if err := h.router.Put(r.Context(), key, req.Value); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeJSON(w, http.StatusRequestTimeout, errorResponse{Error: err.Error()})
			return
		}
		h.metrics.FailedRequests.Add(1)
		writeJSON(w, http.StatusBadGateway, errorResponse{Error: "failed to process request"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *Handlers) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Prevent forwarding loops: if request already forwarded but still not owner, error.
	if r.Header.Get("X-ShardScale-Forwarded") == "true" {
		owner := h.router.KeyOwner(key)
		if owner != h.router.SelfID {
			h.logger.Warn("forwarding loop detected",
				slog.String("key", key),
				slog.String("owner", owner),
				slog.String("self_id", h.router.SelfID),
			)
			h.metrics.FailedRequests.Add(1)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "forwarding loop detected"})
			return
		}
	}

	h.metrics.TotalReads.Add(1)

	value, err := h.router.Get(r.Context(), key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "key not found"})
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeJSON(w, http.StatusRequestTimeout, errorResponse{Error: err.Error()})
			return
		}
		h.metrics.FailedRequests.Add(1)
		writeJSON(w, http.StatusBadGateway, errorResponse{Error: "failed to process request"})
		return
	}

	writeJSON(w, http.StatusOK, getResponse{Key: key, Value: value})
}

func (h *Handlers) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	writeJSON(w, http.StatusOK, h.metrics.Snapshot(h.router.Count()))
}

func (h *Handlers) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func parseKey(path string) (string, bool) {
	const prefix = "/kv/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}

	key := strings.TrimSpace(strings.TrimPrefix(path, prefix))
	if key == "" || strings.Contains(key, "/") {
		return "", false
	}

	return key, true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
