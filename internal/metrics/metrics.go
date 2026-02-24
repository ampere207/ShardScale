package metrics

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	StartTime           time.Time
	TotalRequests       atomic.Uint64
	TotalWrites         atomic.Uint64
	TotalReads          atomic.Uint64
	FailedRequests      atomic.Uint64
	RebalanceInProgress atomic.Bool
	KeysMigrated        atomic.Int64
	MigrationStartedAt  atomic.Int64
	MigrationDurationMs atomic.Int64
}

type Snapshot struct {
	UptimeSeconds       int64  `json:"uptime_seconds"`
	TotalRequests       uint64 `json:"total_requests"`
	TotalReads          uint64 `json:"total_reads"`
	TotalWrites         uint64 `json:"total_writes"`
	FailedRequests      uint64 `json:"failed_requests"`
	TotalKeys           int    `json:"total_keys"`
	RebalanceInProgress bool   `json:"rebalance_in_progress"`
	KeysMigrated        int64  `json:"keys_migrated"`
	MigrationStartedAt  int64  `json:"migration_started_at"`
	MigrationDurationMs int64  `json:"migration_duration_ms"`
}

func New() *Metrics {
	return &Metrics{StartTime: time.Now()}
}

func (m *Metrics) Snapshot(totalKeys int) Snapshot {
	uptime := int64(time.Since(m.StartTime).Seconds())
	if uptime < 0 {
		uptime = 0
	}

	return Snapshot{
		UptimeSeconds:       uptime,
		TotalRequests:       m.TotalRequests.Load(),
		TotalReads:          m.TotalReads.Load(),
		TotalWrites:         m.TotalWrites.Load(),
		FailedRequests:      m.FailedRequests.Load(),
		TotalKeys:           totalKeys,
		RebalanceInProgress: m.RebalanceInProgress.Load(),
		KeysMigrated:        m.KeysMigrated.Load(),
		MigrationStartedAt:  m.MigrationStartedAt.Load(),
		MigrationDurationMs: m.MigrationDurationMs.Load(),
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
