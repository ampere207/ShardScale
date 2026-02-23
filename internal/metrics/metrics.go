package metrics

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	StartTime      time.Time
	TotalRequests  atomic.Uint64
	TotalWrites    atomic.Uint64
	TotalReads     atomic.Uint64
	FailedRequests atomic.Uint64
}

type Snapshot struct {
	UptimeSeconds  int64  `json:"uptime_seconds"`
	TotalRequests  uint64 `json:"total_requests"`
	TotalReads     uint64 `json:"total_reads"`
	TotalWrites    uint64 `json:"total_writes"`
	FailedRequests uint64 `json:"failed_requests"`
	TotalKeys      int    `json:"total_keys"`
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
		UptimeSeconds:  uptime,
		TotalRequests:  m.TotalRequests.Load(),
		TotalReads:     m.TotalReads.Load(),
		TotalWrites:    m.TotalWrites.Load(),
		FailedRequests: m.FailedRequests.Load(),
		TotalKeys:      totalKeys,
	}
}
