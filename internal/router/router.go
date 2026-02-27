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
	writeQuorum       int
	readQuorum        int
	maxQuorumWorkers  int
	quorumTimeout     time.Duration
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

func New(selfID string, peers map[string]string, peersMu *sync.RWMutex, s *store.Store, hashRing *ring.HashRing, replicationFactor int, writeQuorum int, readQuorum int, m *metrics.Metrics, logger *slog.Logger) *Router {
	if peersMu == nil {
		peersMu = &sync.RWMutex{}
	}
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	if writeQuorum < 1 {
		writeQuorum = 1
	}
	if writeQuorum > replicationFactor {
		writeQuorum = replicationFactor
	}
	if readQuorum < 1 {
		readQuorum = 1
	}
	if readQuorum > replicationFactor {
		readQuorum = replicationFactor
	}

	return &Router{
		SelfID:            selfID,
		Peers:             peers,
		peersMu:           peersMu,
		store:             s,
		ring:              hashRing,
		replicationFactor: replicationFactor,
		writeQuorum:       writeQuorum,
		readQuorum:        readQuorum,
		maxQuorumWorkers:  8,
		quorumTimeout:     3 * time.Second,
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

// Put performs a quorum write across owners for the key.
func (r *Router) Put(ctx context.Context, key, value string) error {
	owners := r.getOwners(key)
	if len(owners) == 0 {
		owners = []string{r.SelfID}
	}
	requiredAcks := r.effectiveWriteQuorum(len(owners))

	quorumCtx, cancel := context.WithTimeout(ctx, r.quorumTimeout)
	defer cancel()

	workerCount := minInt(len(owners), r.maxQuorumWorkers)
	jobs := make(chan string, len(owners))
	results := make(chan quorumWriteResult, len(owners))

	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ownerID := range jobs {
				err := r.writeToOwner(quorumCtx, ownerID, key, value)
				result := quorumWriteResult{ownerID: ownerID, err: err}
				select {
				case results <- result:
				case <-quorumCtx.Done():
					return
				}
			}
		}()
	}

	for _, ownerID := range owners {
		jobs <- ownerID
	}
	close(jobs)

	go func() {
		workers.Wait()
		close(results)
	}()

	successes := 0
	completed := 0
	var lastErr error
	primary := owners[0]

	for completed < len(owners) {
		select {
		case <-quorumCtx.Done():
			if successes >= requiredAcks {
				if r.metrics != nil {
					r.metrics.IncSuccessfulQuorumWritesTotal()
				}
				return nil
			}
			if lastErr == nil {
				lastErr = quorumCtx.Err()
			}
			if r.metrics != nil {
				r.metrics.IncQuorumFailuresTotal()
			}
			return &StatusError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("write quorum not satisfied: need %d acks, got %d (%v)", requiredAcks, successes, lastErr)}
		case res, ok := <-results:
			if !ok {
				completed = len(owners)
				break
			}
			completed++

			if res.err == nil {
				successes++
				if res.ownerID != primary && r.metrics != nil {
					r.metrics.IncReplicaWritesTotal()
				}
				if successes >= requiredAcks {
					cancel()
					if r.metrics != nil {
						r.metrics.IncSuccessfulQuorumWritesTotal()
					}
					return nil
				}
				continue
			}

			lastErr = res.err
			r.logger.Warn("quorum write owner failed",
				slog.String("key", key),
				slog.String("owner", res.ownerID),
				slog.String("error", res.err.Error()),
			)
			if res.ownerID != primary && r.metrics != nil {
				r.metrics.IncReplicaFailuresTotal()
			}

			remaining := len(owners) - completed
			if successes+remaining < requiredAcks {
				cancel()
				if r.metrics != nil {
					r.metrics.IncQuorumFailuresTotal()
				}
				return &StatusError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("write quorum not satisfied: need %d acks, got %d (%v)", requiredAcks, successes, lastErr)}
			}
		}
	}

	if successes >= requiredAcks {
		if r.metrics != nil {
			r.metrics.IncSuccessfulQuorumWritesTotal()
		}
		return nil
	}

	if lastErr == nil {
		lastErr = errors.New("insufficient acknowledgements")
	}
	if r.metrics != nil {
		r.metrics.IncQuorumFailuresTotal()
	}
	return &StatusError{Code: http.StatusInternalServerError, Message: fmt.Sprintf("write quorum not satisfied: need %d acks, got %d (%v)", requiredAcks, successes, lastErr)}
}

// Get performs a quorum read across owners for the key.
func (r *Router) Get(ctx context.Context, key string) (string, error) {
	owners := r.getOwners(key)
	if len(owners) == 0 {
		owners = []string{r.SelfID}
	}
	requiredReads := r.effectiveReadQuorum(len(owners))

	quorumCtx, cancel := context.WithTimeout(ctx, r.quorumTimeout)
	defer cancel()

	workerCount := minInt(len(owners), r.maxQuorumWorkers)
	jobs := make(chan string, len(owners))
	results := make(chan quorumReadResult, len(owners))

	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ownerID := range jobs {
				value, err := r.readFromOwner(quorumCtx, ownerID, key)
				result := quorumReadResult{ownerID: ownerID, value: value, err: err}
				select {
				case results <- result:
				case <-quorumCtx.Done():
					return
				}
			}
		}()
	}

	for _, ownerID := range owners {
		jobs <- ownerID
	}
	close(jobs)

	go func() {
		workers.Wait()
		close(results)
	}()

	successes := 0
	notFoundCount := 0
	completed := 0
	firstValue := ""
	haveValue := false
	var lastErr error

	for completed < len(owners) {
		select {
		case <-quorumCtx.Done():
			if successes >= requiredReads && haveValue {
				if r.metrics != nil {
					r.metrics.IncSuccessfulQuorumReadsTotal()
				}
				return firstValue, nil
			}
			if notFoundCount >= requiredReads {
				return "", store.ErrNotFound
			}
			if lastErr == nil {
				lastErr = quorumCtx.Err()
			}
			if r.metrics != nil {
				r.metrics.IncQuorumFailuresTotal()
			}
			return "", &StatusError{Code: http.StatusBadGateway, Message: fmt.Sprintf("read quorum not satisfied: need %d responses, got %d (%v)", requiredReads, successes, lastErr)}
		case res, ok := <-results:
			if !ok {
				completed = len(owners)
				break
			}
			completed++

			if res.err == nil {
				successes++
				if !haveValue {
					haveValue = true
					firstValue = res.value
				}
				if successes >= requiredReads && haveValue {
					cancel()
					if r.metrics != nil {
						r.metrics.IncSuccessfulQuorumReadsTotal()
					}
					return firstValue, nil
				}
				continue
			}

			if errors.Is(res.err, store.ErrNotFound) {
				notFoundCount++
				if notFoundCount >= requiredReads {
					cancel()
					return "", store.ErrNotFound
				}
				continue
			}

			lastErr = res.err
			r.logger.Warn("quorum read owner failed",
				slog.String("key", key),
				slog.String("owner", res.ownerID),
				slog.String("error", res.err.Error()),
			)

			remaining := len(owners) - completed
			if successes+remaining < requiredReads {
				cancel()
				if r.metrics != nil {
					r.metrics.IncQuorumFailuresTotal()
				}
				return "", &StatusError{Code: http.StatusBadGateway, Message: fmt.Sprintf("read quorum not satisfied: need %d responses, got %d (%v)", requiredReads, successes, lastErr)}
			}
		}
	}

	if successes >= requiredReads && haveValue {
		if r.metrics != nil {
			r.metrics.IncSuccessfulQuorumReadsTotal()
		}
		return firstValue, nil
	}
	if notFoundCount >= requiredReads {
		return "", store.ErrNotFound
	}
	if r.metrics != nil {
		r.metrics.IncQuorumFailuresTotal()
	}
	if lastErr == nil {
		lastErr = errors.New("insufficient read responses")
	}
	return "", &StatusError{Code: http.StatusBadGateway, Message: fmt.Sprintf("read quorum not satisfied: need %d responses, got %d (%v)", requiredReads, successes, lastErr)}
}

// Count returns the count of local keys only.
func (r *Router) Count() int {
	return r.store.Count()
}

func (r *Router) DirectPut(ctx context.Context, key, value string) error {
	return r.store.Put(ctx, key, value)
}

func (r *Router) DirectGet(ctx context.Context, key string) (string, error) {
	return r.store.Get(ctx, key)
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

func (r *Router) WriteQuorum() int {
	if r.writeQuorum < 1 {
		return 1
	}
	return r.writeQuorum
}

func (r *Router) ReadQuorum() int {
	if r.readQuorum < 1 {
		return 1
	}
	return r.readQuorum
}

func (r *Router) getOwners(key string) []string {
	owners := r.ring.GetOwners(key, r.replicationFactor)
	if len(owners) == 0 {
		return []string{r.SelfID}
	}
	return owners
}

func (r *Router) effectiveWriteQuorum(ownerCount int) int {
	if ownerCount < 1 {
		return 1
	}
	required := r.writeQuorum
	if required < 1 {
		required = 1
	}
	if required > ownerCount {
		required = ownerCount
	}
	return required
}

func (r *Router) effectiveReadQuorum(ownerCount int) int {
	if ownerCount < 1 {
		return 1
	}
	required := r.readQuorum
	if required < 1 {
		required = 1
	}
	if required > ownerCount {
		required = ownerCount
	}
	return required
}

func (r *Router) writeToOwner(ctx context.Context, ownerID, key, value string) error {
	if ownerID == r.SelfID {
		return r.store.Put(ctx, key, value)
	}
	return r.replicateTo(ctx, ownerID, key, value)
}

func (r *Router) readFromOwner(ctx context.Context, ownerID, key string) (string, error) {
	if ownerID == r.SelfID {
		return r.store.Get(ctx, key)
	}

	r.peersMu.RLock()
	addr, ok := r.Peers[ownerID]
	r.peersMu.RUnlock()
	if !ok {
		return "", fmt.Errorf("owner node not in peers: %s", ownerID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/internal/read/"+key, nil)
	if err != nil {
		return "", fmt.Errorf("create quorum read request failed: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("quorum read request failed: %w", err)
	}
	defer resp.Body.Close()

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
		return "", fmt.Errorf("decode quorum read response failed: %w", err)
	}

	return respData.Value, nil
}

type quorumWriteResult struct {
	ownerID string
	err     error
}

type quorumReadResult struct {
	ownerID string
	value   string
	err     error
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
