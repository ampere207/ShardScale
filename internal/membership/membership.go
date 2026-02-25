package membership

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"shardscale/internal/metrics"
	"shardscale/internal/rebalance"
	"shardscale/internal/ring"
)

type Membership struct {
	SelfID     string
	Peers      map[string]string
	Ring       *ring.HashRing
	Rebalancer *rebalance.Rebalancer
	HTTPClient *http.Client
	mu         sync.RWMutex
	lastSeen   map[string]time.Time
	interval   time.Duration
	timeout    time.Duration
	stopCh     chan struct{}

	logger  *slog.Logger
	metrics *metrics.Metrics
}

type heartbeatResponse struct {
	NodeID    string `json:"node_id"`
	Timestamp int64  `json:"timestamp"`
}

type peerInfo struct {
	id   string
	addr string
}

func New(selfID string, peers map[string]string, ringRef *ring.HashRing, rebalancer *rebalance.Rebalancer, metricsRef *metrics.Metrics, logger *slog.Logger, interval, timeout time.Duration) *Membership {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if timeout <= 0 {
		timeout = 6 * time.Second
	}

	return &Membership{
		SelfID:     selfID,
		Peers:      peers,
		Ring:       ringRef,
		Rebalancer: rebalancer,
		HTTPClient: &http.Client{Timeout: 2 * time.Second},
		lastSeen:   make(map[string]time.Time),
		interval:   interval,
		timeout:    timeout,
		stopCh:     make(chan struct{}),
		logger:     logger,
		metrics:    metricsRef,
	}
}

func (m *Membership) PeerLock() *sync.RWMutex {
	return &m.mu
}

func (m *Membership) Start() {
	now := time.Now()

	m.mu.Lock()
	for nodeID := range m.Peers {
		m.lastSeen[nodeID] = now
	}
	m.mu.Unlock()

	go m.loop()
}

func (m *Membership) Stop() {
	m.mu.Lock()
	if m.stopCh != nil {
		close(m.stopCh)
		m.stopCh = nil
	}
	m.mu.Unlock()
}

func (m *Membership) loop() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.poll()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Membership) poll() {
	peers := m.peerSnapshot()
	if len(peers) == 0 {
		return
	}

	now := time.Now()
	for _, peer := range peers {
		if m.sendHeartbeat(peer) {
			m.updateLastSeen(peer.id, now)
			continue
		}

		if m.shouldRemove(peer.id, now) {
			m.removePeer(peer.id)
		}
	}
}

func (m *Membership) peerSnapshot() []peerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]peerInfo, 0, len(m.Peers))
	for nodeID, addr := range m.Peers {
		if nodeID == m.SelfID {
			continue
		}
		peers = append(peers, peerInfo{id: nodeID, addr: addr})
	}

	return peers
}

func (m *Membership) sendHeartbeat(peer peerInfo) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+peer.addr+"/internal/heartbeat", nil)
	if err != nil {
		return false
	}

	resp, err := m.HTTPClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var payload heartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return false
	}

	if payload.NodeID != "" && payload.NodeID != peer.id {
		m.logger.Warn("heartbeat node_id mismatch",
			slog.String("expected", peer.id),
			slog.String("received", payload.NodeID),
		)
		return false
	}

	return true
}

func (m *Membership) updateLastSeen(nodeID string, now time.Time) {
	m.mu.Lock()
	if _, exists := m.Peers[nodeID]; exists {
		m.lastSeen[nodeID] = now
	}
	m.mu.Unlock()
}

func (m *Membership) shouldRemove(nodeID string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	last, exists := m.lastSeen[nodeID]
	if !exists {
		m.lastSeen[nodeID] = now
		return false
	}

	return now.Sub(last) > m.timeout
}

func (m *Membership) removePeer(nodeID string) {
	if nodeID == "" || nodeID == m.SelfID {
		return
	}

	var nodes []string
	var shouldRebalance bool

	m.mu.Lock()
	if _, exists := m.Peers[nodeID]; !exists {
		m.mu.Unlock()
		return
	}

	delete(m.Peers, nodeID)
	delete(m.lastSeen, nodeID)

	nodes = make([]string, 0, len(m.Peers))
	for id := range m.Peers {
		nodes = append(nodes, id)
	}
	m.Ring.Rebuild(nodes)
	m.metrics.IncFailedNodesDetected()
	m.metrics.IncMembershipChanges()
	shouldRebalance = true
	m.mu.Unlock()

	m.logger.Warn("peer removed due to heartbeat timeout",
		slog.String("node_id", nodeID),
	)

	if shouldRebalance && m.Rebalancer != nil {
		m.Rebalancer.StartRebalance()
	}
}
