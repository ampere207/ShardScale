package node

import (
	"log/slog"
	"sync"

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
}

func New(id, addr string, peers map[string]string, virtualNodes int, m *metrics.Metrics, logger *slog.Logger) *Node {
	s := store.New()
	peersMu := &sync.RWMutex{}

	// Build hash ring with all nodes (self + peers)
	hashRing := ring.NewHashRing(virtualNodes)
	nodeList := make([]string, 0)
	for nodeID := range peers {
		nodeList = append(nodeList, nodeID)
	}
	hashRing.Rebuild(nodeList)

	r := router.New(id, peers, peersMu, s, hashRing, logger)
	rebalancer := rebalance.New(id, hashRing, s, peers, peersMu, m, logger, 10)

	return &Node{
		ID:         id,
		Addr:       addr,
		Peers:      peers,
		PeersMu:    peersMu,
		Store:      s,
		Router:     r,
		Ring:       hashRing,
		Rebalancer: rebalancer,
	}
}
