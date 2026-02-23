package node

import (
	"log/slog"

	"shardscale/internal/ring"
	"shardscale/internal/router"
	"shardscale/internal/store"
)

// Node represents a cluster node with its identity, peers, and routing/storage components.
type Node struct {
	ID     string
	Addr   string
	Peers  map[string]string // nodeID -> address
	Store  *store.Store
	Router *router.Router
	Ring   *ring.HashRing
}

func New(id, addr string, peers map[string]string, virtualNodes int, logger *slog.Logger) *Node {
	s := store.New()

	// Build hash ring with all nodes (self + peers)
	hashRing := ring.NewHashRing(virtualNodes)
	nodeList := make([]string, 0)
	for nodeID := range peers {
		nodeList = append(nodeList, nodeID)
	}
	hashRing.Rebuild(nodeList)

	r := router.New(id, peers, s, hashRing, logger)

	return &Node{
		ID:     id,
		Addr:   addr,
		Peers:  peers,
		Store:  s,
		Router: r,
		Ring:   hashRing,
	}
}
