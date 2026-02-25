package metrics

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	StartTime              time.Time
	TotalRequests          atomic.Uint64
	TotalWrites            atomic.Uint64
	TotalReads             atomic.Uint64
	FailedRequests         atomic.Uint64
	ReplicationFactor      atomic.Int64
	ReplicaWritesTotal     atomic.Uint64
	ReplicaFailuresTotal   atomic.Uint64
	ReplicaSyncEventsTotal atomic.Uint64
	RebalanceInProgress    atomic.Bool
	KeysMigrated           atomic.Int64
	MigrationStartedAt     atomic.Int64
	MigrationDurationMs    atomic.Int64
	FailedNodesDetected    atomic.Uint64
	MembershipChanges      atomic.Uint64
}

type Snapshot struct {
	UptimeSeconds          int64  `json:"uptime_seconds"`
	TotalRequests          uint64 `json:"total_requests"`
	TotalReads             uint64 `json:"total_reads"`
	TotalWrites            uint64 `json:"total_writes"`
	FailedRequests         uint64 `json:"failed_requests"`
	TotalKeys              int    `json:"total_keys"`
	ActiveNodes            int    `json:"active_nodes"`
	ReplicationFactor      int64  `json:"replication_factor"`
	ReplicaWritesTotal     uint64 `json:"replica_writes_total"`
	ReplicaFailuresTotal   uint64 `json:"replica_failures_total"`
	ReplicaSyncEventsTotal uint64 `json:"replica_sync_events_total"`
	RebalanceInProgress    bool   `json:"rebalance_in_progress"`
	KeysMigrated           int64  `json:"keys_migrated"`
	MigrationStartedAt     int64  `json:"migration_started_at"`
	MigrationDurationMs    int64  `json:"migration_duration_ms"`
	FailedNodesDetected    uint64 `json:"failed_nodes_detected"`
	MembershipChanges      uint64 `json:"membership_changes"`
}

func New() *Metrics {
	m := &Metrics{StartTime: time.Now()}
	m.ReplicationFactor.Store(1)
	return m
}

func (m *Metrics) Snapshot(totalKeys int, activeNodes int, replicationFactor int) Snapshot {
	uptime := int64(time.Since(m.StartTime).Seconds())
	if uptime < 0 {
		uptime = 0
	}

	if replicationFactor < 1 {
		replicationFactor = 1
	}
	if activeNodes > 0 && replicationFactor > activeNodes {
		replicationFactor = activeNodes
	}

	return Snapshot{
		UptimeSeconds:          uptime,
		TotalRequests:          m.TotalRequests.Load(),
		TotalReads:             m.TotalReads.Load(),
		TotalWrites:            m.TotalWrites.Load(),
		FailedRequests:         m.FailedRequests.Load(),
		TotalKeys:              totalKeys,
		ActiveNodes:            activeNodes,
		ReplicationFactor:      int64(replicationFactor),
		ReplicaWritesTotal:     m.ReplicaWritesTotal.Load(),
		ReplicaFailuresTotal:   m.ReplicaFailuresTotal.Load(),
		ReplicaSyncEventsTotal: m.ReplicaSyncEventsTotal.Load(),
		RebalanceInProgress:    m.RebalanceInProgress.Load(),
		KeysMigrated:           m.KeysMigrated.Load(),
		MigrationStartedAt:     m.MigrationStartedAt.Load(),
		MigrationDurationMs:    m.MigrationDurationMs.Load(),
		FailedNodesDetected:    m.FailedNodesDetected.Load(),
		MembershipChanges:      m.MembershipChanges.Load(),
	}
}

func (m *Metrics) MarkRebalanceStarted(start time.Time) {
	m.RebalanceInProgress.Store(true)
	m.KeysMigrated.Store(0)
	m.MigrationStartedAt.Store(start.UnixMilli())
	m.MigrationDurationMs.Store(0)
}

func (m *Metrics) SetKeysMigrated(count int64) {
	m.KeysMigrated.Store(count)
}

func (m *Metrics) MarkRebalanceCompleted(start time.Time, duration time.Duration, migrated int64) {
	m.RebalanceInProgress.Store(false)
	m.KeysMigrated.Store(migrated)
	m.MigrationStartedAt.Store(start.UnixMilli())
	m.MigrationDurationMs.Store(duration.Milliseconds())
}

func (m *Metrics) IncFailedNodesDetected() {
	m.FailedNodesDetected.Add(1)
}

func (m *Metrics) IncMembershipChanges() {
	m.MembershipChanges.Add(1)
}

func (m *Metrics) SetReplicationFactor(replicationFactor int) {
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	m.ReplicationFactor.Store(int64(replicationFactor))
}

func (m *Metrics) GetReplicationFactor() int {
	value := int(m.ReplicationFactor.Load())
	if value < 1 {
		return 1
	}
	return value
}

func (m *Metrics) IncReplicaWritesTotal() {
	m.ReplicaWritesTotal.Add(1)
}

func (m *Metrics) IncReplicaFailuresTotal() {
	m.ReplicaFailuresTotal.Add(1)
}

func (m *Metrics) IncReplicaSyncEventsTotal() {
	m.ReplicaSyncEventsTotal.Add(1)
}
