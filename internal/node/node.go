package node

import (
	"log/slog"
	"sync"
	"time"

	"shardscale/internal/membership"
	"shardscale/internal/metrics"
	"shardscale/internal/rebalance"
	"shardscale/internal/ring"
	"shardscale/internal/router"
	"shardscale/internal/store"
)

// Node represents a cluster node with its identity, peers, and routing/storage components.
type Node struct {
	ID         string
	Addr       string
	Peers      map[string]string // nodeID -> address
	PeersMu    *sync.RWMutex
	Store      *store.Store
	Router     *router.Router
	Ring       *ring.HashRing
	Rebalancer *rebalance.Rebalancer
	Membership *membership.Membership
}

func New(id, addr string, peers map[string]string, virtualNodes int, replicationFactor int, writeQuorum int, readQuorum int, heartbeatInterval, heartbeatTimeout time.Duration, m *metrics.Metrics, logger *slog.Logger) *Node {
	s := store.New()

	// Build hash ring with all nodes (self + peers)
	hashRing := ring.NewHashRing(virtualNodes)
	nodeList := make([]string, 0)
	for nodeID := range peers {
		nodeList = append(nodeList, nodeID)
	}
	hashRing.Rebuild(nodeList)

	member := membership.New(id, peers, hashRing, nil, m, logger, heartbeatInterval, heartbeatTimeout)
	r := router.New(id, peers, member.PeerLock(), s, hashRing, replicationFactor, writeQuorum, readQuorum, m, logger)
	rebalancer := rebalance.New(id, hashRing, s, peers, member.PeerLock(), m, logger, 10)
	member.Rebalancer = rebalancer

	return &Node{
		ID:         id,
		Addr:       addr,
		Peers:      peers,
		PeersMu:    member.PeerLock(),
		Store:      s,
		Router:     r,
		Ring:       hashRing,
		Rebalancer: rebalancer,
		Membership: member,
	}
}
