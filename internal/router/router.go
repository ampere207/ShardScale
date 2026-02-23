package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"shardscale/internal/ring"
	"shardscale/internal/store"
)

// Router handles key ownership determination via consistent hashing and request routing.
type Router struct {
	SelfID     string
	Peers      map[string]string // nodeID -> address
	store      *store.Store
	ring       *ring.HashRing
	httpClient *http.Client
	logger     *slog.Logger
}

func New(selfID string, peers map[string]string, s *store.Store, hashRing *ring.HashRing, logger *slog.Logger) *Router {
	return &Router{
		SelfID: selfID,
		Peers:  peers,
		store:  s,
		ring:   hashRing,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		logger: logger,
	}
}

// KeyOwner determines which node owns the given key using consistent hashing.
func (r *Router) KeyOwner(key string) string {
	owner := r.ring.GetOwner(key)

	// Fallback to self if ring is empty or returns empty owner
	if owner == "" {
		r.logger.Warn("hash ring returned empty owner, defaulting to self",
			slog.String("key", key),
			slog.String("self_id", r.SelfID),
		)
		return r.SelfID
	}

	return owner
}

// Put stores a value locally or forwards to owner if necessary.
func (r *Router) Put(ctx context.Context, key, value string) error {
	owner := r.KeyOwner(key)

	if owner == r.SelfID {
		r.logger.Debug("put: handling locally", slog.String("key", key))
		return r.store.Put(ctx, key, value)
	}

	r.logger.Info("put: forwarding to owner",
		slog.String("key", key),
		slog.String("owner", owner),
	)
	return r.forwardPut(ctx, owner, key, value)
}

// Get retrieves a value locally or forwards to owner if necessary.
func (r *Router) Get(ctx context.Context, key string) (string, error) {
	owner := r.KeyOwner(key)

	if owner == r.SelfID {
		r.logger.Debug("get: handling locally", slog.String("key", key))
		return r.store.Get(ctx, key)
	}

	r.logger.Info("get: forwarding to owner",
		slog.String("key", key),
		slog.String("owner", owner),
	)
	return r.forwardGet(ctx, owner, key)
}

// Count returns the count of local keys only.
func (r *Router) Count() int {
	return r.store.Count()
}

// forwardPut sends a PUT request to the owner node.
func (r *Router) forwardPut(ctx context.Context, owner, key, value string) error {
	addr, ok := r.Peers[owner]
	if !ok {
		return fmt.Errorf("owner node not in peers: %s", owner)
	}

	payload := map[string]interface{}{"value": value}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal request failed: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://"+addr+"/kv/"+key, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create forwarded request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ShardScale-Forwarded", "true")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("forwarding put failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("forwarded put returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// forwardGet sends a GET request to the owner node and returns the value.
func (r *Router) forwardGet(ctx context.Context, owner, key string) (string, error) {
	addr, ok := r.Peers[owner]
	if !ok {
		return "", fmt.Errorf("owner node not in peers: %s", owner)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/kv/"+key, nil)
	if err != nil {
		return "", fmt.Errorf("create forwarded request failed: %w", err)
	}

	req.Header.Set("X-ShardScale-Forwarded", "true")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("forwarding get failed: %w", err)
	}
	defer resp.Body.Close()

	// Map 404 to ErrNotFound so caller sees consistent behavior
	if resp.StatusCode == http.StatusNotFound {
		return "", store.ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("forwarded get returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var respData struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return "", fmt.Errorf("decode forwarded response failed: %w", err)
	}

	return respData.Value, nil
}
