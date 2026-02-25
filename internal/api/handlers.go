package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"shardscale/internal/metrics"
	"shardscale/internal/rebalance"
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

type transferRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type joinRequest struct {
	NodeID   string `json:"node_id"`
	NodeAddr string `json:"node_addr"`
}

type heartbeatResponse struct {
	NodeID    string `json:"node_id"`
	Timestamp int64  `json:"timestamp"`
}

type Handlers struct {
	router     *router.Router
	rebalancer *rebalance.Rebalancer
	metrics    *metrics.Metrics
	logger     *slog.Logger
}

func NewHandlers(r *router.Router, rb *rebalance.Rebalancer, m *metrics.Metrics, logger *slog.Logger) *Handlers {
	return &Handlers{router: r, rebalancer: rb, metrics: m, logger: logger}
}

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("/kv/", h.handleKV)
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/internal/transfer", h.handleInternalTransfer)
	mux.HandleFunc("/internal/replicate", h.handleInternalReplicate)
	mux.HandleFunc("/internal/join", h.handleInternalJoin)
	mux.HandleFunc("/internal/heartbeat", h.handleInternalHeartbeat)
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
		var statusErr *router.StatusError
		if errors.As(err, &statusErr) {
			h.metrics.FailedRequests.Add(1)
			writeJSON(w, statusErr.Code, errorResponse{Error: "failed to process request"})
			return
		}
		h.metrics.FailedRequests.Add(1)
		if h.router.KeyOwner(key) == h.router.SelfID {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to process request"})
			return
		}
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
		var statusErr *router.StatusError
		if errors.As(err, &statusErr) {
			h.metrics.FailedRequests.Add(1)
			writeJSON(w, statusErr.Code, errorResponse{Error: "failed to process request"})
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

	activeNodes := len(h.router.PeerSnapshot())
	writeJSON(w, http.StatusOK, h.metrics.Snapshot(h.router.Count(), activeNodes, h.router.ReplicationFactor()))
}

func (h *Handlers) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *Handlers) handleInternalTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	h.metrics.TotalWrites.Add(1)
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req transferRequest
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "request body must contain a single JSON object"})
		return
	}

	req.Key = strings.TrimSpace(req.Key)
	if req.Key == "" || strings.Contains(req.Key, "/") {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "key must be non-empty and must not contain '/'"})
		return
	}

	if strings.TrimSpace(req.Value) == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "value must be non-empty"})
		return
	}

	if err := h.router.DirectPut(r.Context(), req.Key, req.Value); err != nil {
		h.metrics.FailedRequests.Add(1)
		writeJSON(w, http.StatusBadGateway, errorResponse{Error: "failed to store transfer"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *Handlers) handleInternalReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req transferRequest
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "request body must contain a single JSON object"})
		return
	}

	req.Key = strings.TrimSpace(req.Key)
	if req.Key == "" || strings.Contains(req.Key, "/") {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "key must be non-empty and must not contain '/'"})
		return
	}

	if strings.TrimSpace(req.Value) == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "value must be non-empty"})
		return
	}

	if err := h.router.DirectPut(r.Context(), req.Key, req.Value); err != nil {
		h.metrics.FailedRequests.Add(1)
		writeJSON(w, http.StatusBadGateway, errorResponse{Error: "failed to store replica"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *Handlers) handleInternalJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var req joinRequest
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "request body must contain a single JSON object"})
		return
	}

	req.NodeID = strings.TrimSpace(req.NodeID)
	req.NodeAddr = strings.TrimSpace(req.NodeAddr)
	if req.NodeID == "" || req.NodeAddr == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "node_id and node_addr are required"})
		return
	}

	changed := h.router.AddOrUpdatePeer(req.NodeID, req.NodeAddr)
	if changed {
		h.metrics.IncMembershipChanges()
		h.rebalancer.RebuildRingFromPeers()
		h.rebalancer.StartRebalance()
		h.logger.Info("cluster join processed",
			slog.String("node_id", req.NodeID),
			slog.String("node_addr", req.NodeAddr),
		)
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (h *Handlers) handleInternalHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	writeJSON(w, http.StatusOK, heartbeatResponse{
		NodeID:    h.router.SelfID,
		Timestamp: time.Now().UnixMilli(),
	})
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
