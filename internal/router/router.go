package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"shardscale/internal/metrics"
	"shardscale/internal/ring"
	"shardscale/internal/store"
)

// Router handles key ownership determination via consistent hashing and request routing.
type Router struct {
	SelfID            string
	Peers             map[string]string // nodeID -> address
	peersMu           *sync.RWMutex
	store             *store.Store
	ring              *ring.HashRing
	replicationFactor int
	metrics           *metrics.Metrics
	httpClient        *http.Client
	logger            *slog.Logger
}

type StatusError struct {
	Code    int
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("status %d: %s", e.Code, e.Message)
}

func New(selfID string, peers map[string]string, peersMu *sync.RWMutex, s *store.Store, hashRing *ring.HashRing, replicationFactor int, m *metrics.Metrics, logger *slog.Logger) *Router {
	if peersMu == nil {
		peersMu = &sync.RWMutex{}
	}
	if replicationFactor < 1 {
		replicationFactor = 1
	}

	return &Router{
		SelfID:            selfID,
		Peers:             peers,
		peersMu:           peersMu,
		store:             s,
		ring:              hashRing,
		replicationFactor: replicationFactor,
		metrics:           m,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		logger: logger,
	}
}

// KeyOwner determines which node owns the given key using consistent hashing.
func (r *Router) KeyOwner(key string) string {
	owners := r.getOwners(key)
	if len(owners) == 0 {
		r.logger.Warn("hash ring returned empty owners, defaulting to self",
			slog.String("key", key),
			slog.String("self_id", r.SelfID),
		)
		return r.SelfID
	}

	return owners[0]
}

// Put stores a value locally or forwards to owner if necessary.
func (r *Router) Put(ctx context.Context, key, value string) error {
	owners := r.getOwners(key)
	if len(owners) == 0 {
		owners = []string{r.SelfID}
	}

	primary := owners[0]

	if primary != r.SelfID {
		r.logger.Info("put: forwarding to primary",
			slog.String("key", key),
			slog.String("primary", primary),
		)
		return r.forwardPut(ctx, primary, key, value)
	}

	r.logger.Debug("put: handling as primary", slog.String("key", key))
	if err := r.store.Put(ctx, key, value); err != nil {
		return err
	}

	for _, replicaID := range owners[1:] {
		if err := r.replicateTo(ctx, replicaID, key, value); err != nil {
			r.logger.Error("replica write failed",
				slog.String("key", key),
				slog.String("replica", replicaID),
				slog.String("error", err.Error()),
			)
			if r.metrics != nil {
				r.metrics.IncReplicaFailuresTotal()
			}
			return fmt.Errorf("replication failed for replica %s: %w", replicaID, err)
		}
		if r.metrics != nil {
			r.metrics.IncReplicaWritesTotal()
		}
	}

	return nil
}

// Get retrieves a value locally or forwards to owner if necessary.
func (r *Router) Get(ctx context.Context, key string) (string, error) {
	owners := r.getOwners(key)
	if len(owners) == 0 {
		owners = []string{r.SelfID}
	}
	primary := owners[0]

	if primary == r.SelfID {
		r.logger.Debug("get: handling as primary", slog.String("key", key))
		return r.store.Get(ctx, key)
	}

	r.logger.Info("get: forwarding to primary",
		slog.String("key", key),
		slog.String("primary", primary),
	)

	value, err := r.forwardGet(ctx, primary, key)
	if err == nil {
		return value, nil
	}
	if errors.Is(err, store.ErrNotFound) {
		return "", store.ErrNotFound
	}

	for _, ownerID := range owners[1:] {
		if ownerID == r.SelfID {
			fallbackValue, localErr := r.store.Get(ctx, key)
			if localErr == nil {
				return fallbackValue, nil
			}
			if errors.Is(localErr, store.ErrNotFound) {
				continue
			}
			return "", localErr
		}

		fallbackValue, fallbackErr := r.forwardGet(ctx, ownerID, key)
		if fallbackErr == nil {
			return fallbackValue, nil
		}
		if errors.Is(fallbackErr, store.ErrNotFound) {
			continue
		}
	}

	return "", err
}

// Count returns the count of local keys only.
func (r *Router) Count() int {
	return r.store.Count()
}

func (r *Router) DirectPut(ctx context.Context, key, value string) error {
	return r.store.Put(ctx, key, value)
}

func (r *Router) AddOrUpdatePeer(nodeID, addr string) bool {
	if nodeID == "" || addr == "" {
		return false
	}

	r.peersMu.Lock()
	defer r.peersMu.Unlock()

	current, exists := r.Peers[nodeID]
	if exists && current == addr {
		return false
	}

	r.Peers[nodeID] = addr
	return true
}

func (r *Router) PeerSnapshot() map[string]string {
	r.peersMu.RLock()
	defer r.peersMu.RUnlock()

	copyMap := make(map[string]string, len(r.Peers))
	for nodeID, addr := range r.Peers {
		copyMap[nodeID] = addr
	}

	return copyMap
}

func (r *Router) ReplicationFactor() int {
	if r.replicationFactor < 1 {
		return 1
	}
	return r.replicationFactor
}

func (r *Router) getOwners(key string) []string {
	owners := r.ring.GetOwners(key, r.replicationFactor)
	if len(owners) == 0 {
		return []string{r.SelfID}
	}
	return owners
}

// forwardPut sends a PUT request to the owner node.
func (r *Router) forwardPut(ctx context.Context, owner, key, value string) error {
	r.peersMu.RLock()
	addr, ok := r.Peers[owner]
	r.peersMu.RUnlock()
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
		return &StatusError{Code: resp.StatusCode, Message: string(respBody)}
	}

	return nil
}

// forwardGet sends a GET request to the owner node and returns the value.
func (r *Router) forwardGet(ctx context.Context, owner, key string) (string, error) {
	r.peersMu.RLock()
	addr, ok := r.Peers[owner]
	r.peersMu.RUnlock()
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
		return "", &StatusError{Code: resp.StatusCode, Message: string(respBody)}
	}

	var respData struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return "", fmt.Errorf("decode forwarded response failed: %w", err)
	}

	return respData.Value, nil
}

func (r *Router) replicateTo(ctx context.Context, replicaID, key, value string) error {
	r.peersMu.RLock()
	addr, ok := r.Peers[replicaID]
	r.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("replica node not in peers: %s", replicaID)
	}

	payload := map[string]string{"key": key, "value": value}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal replication request failed: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/internal/replicate", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create replication request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("replication request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("replication returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
