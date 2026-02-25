package rebalance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"shardscale/internal/metrics"
	"shardscale/internal/ring"
	"shardscale/internal/store"
)

type Rebalancer struct {
	NodeID     string
	Ring       *ring.HashRing
	Store      *store.Store
	Peers      map[string]string
	HTTPClient *http.Client

	mu         sync.Mutex
	inProgress bool
	migrated   atomic.Int64

	peersMu     *sync.RWMutex
	logger      *slog.Logger
	metrics     *metrics.Metrics
	workerCount int
	retryLimit  int
}

type transferTask struct {
	owners []string
	key    string
	value  string
}

type transferRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func New(nodeID string, hashRing *ring.HashRing, s *store.Store, peers map[string]string, peersMu *sync.RWMutex, m *metrics.Metrics, logger *slog.Logger, workerCount int) *Rebalancer {
	if workerCount <= 0 {
		workerCount = 10
	}

	return &Rebalancer{
		NodeID:      nodeID,
		Ring:        hashRing,
		Store:       s,
		Peers:       peers,
		HTTPClient:  &http.Client{Timeout: 3 * time.Second},
		peersMu:     peersMu,
		logger:      logger,
		metrics:     m,
		workerCount: workerCount,
		retryLimit:  3,
		inProgress:  false,
	}
}

func (r *Rebalancer) ReplicationFactor() int {
	if r.metrics == nil {
		return 1
	}
	return r.metrics.GetReplicationFactor()
}

func (r *Rebalancer) StartRebalance() {
	r.mu.Lock()
	if r.inProgress {
		r.mu.Unlock()
		r.logger.Info("rebalance already in progress; skipping trigger")
		return
	}

	r.inProgress = true
	r.migrated.Store(0)
	startedAt := time.Now()
	r.mu.Unlock()

	r.metrics.MarkRebalanceStarted(startedAt)
	go r.run(startedAt)
}

func (r *Rebalancer) IsInProgress() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inProgress
}

func (r *Rebalancer) MigratedCount() int64 {
	return r.migrated.Load()
}

func (r *Rebalancer) AddOrUpdatePeer(nodeID, addr string) bool {
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

func (r *Rebalancer) RemovePeer(nodeID string) bool {
	if nodeID == "" {
		return false
	}

	r.peersMu.Lock()
	defer r.peersMu.Unlock()

	if _, exists := r.Peers[nodeID]; !exists {
		return false
	}

	delete(r.Peers, nodeID)
	return true
}

func (r *Rebalancer) RebuildRingFromPeers() {
	r.peersMu.RLock()
	nodes := make([]string, 0, len(r.Peers))
	for nodeID := range r.Peers {
		nodes = append(nodes, nodeID)
	}
	r.peersMu.RUnlock()

	r.Ring.Rebuild(nodes)
}

func (r *Rebalancer) run(startedAt time.Time) {
	snapshot := r.Store.Snapshot()
	jobs := make(chan transferTask, 1024)
	var workers sync.WaitGroup

	for workerID := 0; workerID < r.workerCount; workerID++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for task := range jobs {
				r.processTask(task)
			}
		}()
	}

	queued := 0
	for key, value := range snapshot {
		owners := r.Ring.GetOwners(key, r.ReplicationFactor())
		if len(owners) == 0 {
			continue
		}

		jobs <- transferTask{owners: owners, key: key, value: value}
		queued++
	}
	close(jobs)

	workers.Wait()

	duration := time.Since(startedAt)
	migrated := r.migrated.Load()

	r.mu.Lock()
	r.inProgress = false
	r.mu.Unlock()

	r.metrics.MarkRebalanceCompleted(startedAt, duration, migrated)
	r.logger.Info("rebalance completed",
		slog.Int("queued", queued),
		slog.Int64("migrated", migrated),
		slog.Int64("duration_ms", duration.Milliseconds()),
	)
}

func (r *Rebalancer) processTask(task transferTask) {
	if len(task.owners) == 0 {
		return
	}

	primary := task.owners[0]
	if primary == "" {
		return
	}

	if !containsOwner(task.owners, r.NodeID) {
		r.transferOutToPrimary(task, primary)
		return
	}

	if primary != r.NodeID {
		return
	}

	if r.ensureReplicas(task.key, task.value, task.owners[1:]) {
		r.metrics.IncReplicaSyncEventsTotal()
	}
}

func (r *Rebalancer) transferOutToPrimary(task transferTask, primary string) {
	if !r.transferWithRetry(primary, task.key, task.value) {
		r.logger.Warn("key transfer failed after retries; key retained locally",
			slog.String("key", task.key),
			slog.String("target_owner", primary),
		)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	currentValue, err := r.Store.Get(ctx, task.key)
	cancel()
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return
		}
		r.logger.Warn("post-transfer local validation failed",
			slog.String("key", task.key),
			slog.String("error", err.Error()),
		)
		return
	}

	if currentValue != task.value {
		r.logger.Debug("key changed during migration; skip local delete",
			slog.String("key", task.key),
		)
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	err = r.Store.Delete(ctx, task.key)
	cancel()
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			r.logger.Warn("failed deleting key after successful transfer",
				slog.String("key", task.key),
				slog.String("error", err.Error()),
			)
		}
		return
	}

	migrated := r.migrated.Add(1)
	r.metrics.SetKeysMigrated(migrated)
}

func (r *Rebalancer) ensureReplicas(key, value string, replicas []string) bool {
	for _, replicaID := range replicas {
		if replicaID == "" || replicaID == r.NodeID {
			continue
		}

		if !r.replicateWithRetry(replicaID, key, value) {
			r.metrics.IncReplicaFailuresTotal()
			r.logger.Warn("replica sync failed during rebalance",
				slog.String("key", key),
				slog.String("replica", replicaID),
			)
			return false
		}
		r.metrics.IncReplicaWritesTotal()
	}

	return true
}

func (r *Rebalancer) replicateWithRetry(replicaID, key, value string) bool {
	for attempt := 1; attempt <= r.retryLimit; attempt++ {
		if err := r.replicateOnce(replicaID, key, value); err == nil {
			return true
		}
		time.Sleep(time.Duration(attempt) * 150 * time.Millisecond)
	}

	return false
}

func (r *Rebalancer) replicateOnce(replicaID, key, value string) error {
	r.peersMu.RLock()
	addr, ok := r.Peers[replicaID]
	r.peersMu.RUnlock()
	if !ok {
		return errors.New("replica owner is not in peer map")
	}

	payload, err := json.Marshal(transferRequest{Key: key, Value: value})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/internal/replicate", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("replication endpoint returned non-200")
	}

	return nil
}

func containsOwner(owners []string, nodeID string) bool {
	for _, owner := range owners {
		if owner == nodeID {
			return true
		}
	}
	return false
}

func (r *Rebalancer) transferWithRetry(owner, key, value string) bool {
	for attempt := 1; attempt <= r.retryLimit; attempt++ {
		if err := r.transferOnce(owner, key, value); err == nil {
			return true
		} else {
			r.logger.Warn("key transfer attempt failed",
				slog.String("key", key),
				slog.String("target_owner", owner),
				slog.Int("attempt", attempt),
				slog.String("error", err.Error()),
			)
		}
		time.Sleep(time.Duration(attempt) * 150 * time.Millisecond)
	}

	return false
}

func (r *Rebalancer) transferOnce(owner, key, value string) error {
	r.peersMu.RLock()
	addr, ok := r.Peers[owner]
	r.peersMu.RUnlock()
	if !ok {
		return errors.New("target owner is not in peer map")
	}

	payload, err := json.Marshal(transferRequest{Key: key, Value: value})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/internal/transfer", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("transfer endpoint returned non-200")
	}

	return nil
}
