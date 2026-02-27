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
	WriteQuorum            atomic.Int64
	ReadQuorum             atomic.Int64
	ReplicaWritesTotal     atomic.Uint64
	ReplicaFailuresTotal   atomic.Uint64
	ReplicaSyncEventsTotal atomic.Uint64
	QuorumFailuresTotal    atomic.Uint64
	SuccessfulQuorumWrites atomic.Uint64
	SuccessfulQuorumReads  atomic.Uint64
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
	WriteQuorum            int64  `json:"write_quorum"`
	ReadQuorum             int64  `json:"read_quorum"`
	ReplicaWritesTotal     uint64 `json:"replica_writes_total"`
	ReplicaFailuresTotal   uint64 `json:"replica_failures_total"`
	ReplicaSyncEventsTotal uint64 `json:"replica_sync_events_total"`
	QuorumFailuresTotal    uint64 `json:"quorum_failures_total"`
	SuccessfulQuorumWrites uint64 `json:"successful_quorum_writes_total"`
	SuccessfulQuorumReads  uint64 `json:"successful_quorum_reads_total"`
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
	m.WriteQuorum.Store(1)
	m.ReadQuorum.Store(1)
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

	writeQuorum := int(m.WriteQuorum.Load())
	if writeQuorum < 1 {
		writeQuorum = 1
	}
	if writeQuorum > replicationFactor {
		writeQuorum = replicationFactor
	}

	readQuorum := int(m.ReadQuorum.Load())
	if readQuorum < 1 {
		readQuorum = 1
	}
	if readQuorum > replicationFactor {
		readQuorum = replicationFactor
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
		WriteQuorum:            int64(writeQuorum),
		ReadQuorum:             int64(readQuorum),
		ReplicaWritesTotal:     m.ReplicaWritesTotal.Load(),
		ReplicaFailuresTotal:   m.ReplicaFailuresTotal.Load(),
		ReplicaSyncEventsTotal: m.ReplicaSyncEventsTotal.Load(),
		QuorumFailuresTotal:    m.QuorumFailuresTotal.Load(),
		SuccessfulQuorumWrites: m.SuccessfulQuorumWrites.Load(),
		SuccessfulQuorumReads:  m.SuccessfulQuorumReads.Load(),
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

func (m *Metrics) SetWriteQuorum(writeQuorum int) {
	if writeQuorum < 1 {
		writeQuorum = 1
	}
	m.WriteQuorum.Store(int64(writeQuorum))
}

func (m *Metrics) SetReadQuorum(readQuorum int) {
	if readQuorum < 1 {
		readQuorum = 1
	}
	m.ReadQuorum.Store(int64(readQuorum))
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

func (m *Metrics) IncQuorumFailuresTotal() {
	m.QuorumFailuresTotal.Add(1)
}

func (m *Metrics) IncSuccessfulQuorumWritesTotal() {
	m.SuccessfulQuorumWrites.Add(1)
}

func (m *Metrics) IncSuccessfulQuorumReadsTotal() {
	m.SuccessfulQuorumReads.Add(1)
}
