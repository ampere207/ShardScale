package ring

import (
	"sort"
	"sync"
)

// HashRing implements a consistent hash ring with virtual nodes.
// It provides thread-safe, O(log N) ownership lookups for distributed key placement.
type HashRing struct {
	mu           sync.RWMutex
	sortedHashes []uint32
	hashToNode   map[uint32]string
	virtualNodes int
	nodeSet      map[string]bool // Track which physical nodes exist
}

// NewHashRing creates a new hash ring with configurable virtual nodes.
func NewHashRing(virtualNodes int) *HashRing {
	if virtualNodes < 1 {
		virtualNodes = 50
	}
	return &HashRing{
		sortedHashes: make([]uint32, 0),
		hashToNode:   make(map[uint32]string),
		virtualNodes: virtualNodes,
		nodeSet:      make(map[string]bool),
	}
}

// AddNode adds a physical node to the ring, creating virtualNodes entries.
func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.nodeSet[nodeID] {
		return // Node already exists
	}

	hr.nodeSet[nodeID] = true

	// Add virtual node hashes
	for i := 0; i < hr.virtualNodes; i++ {
		vNodeKey := fnv1aKey(nodeID, i)
		hash := fnv1a32(vNodeKey)
		hr.hashToNode[hash] = nodeID
		hr.sortedHashes = append(hr.sortedHashes, hash)
	}

	// Re-sort after additions
	sort.Slice(hr.sortedHashes, func(i, j int) bool {
		return hr.sortedHashes[i] < hr.sortedHashes[j]
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the ring.
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if !hr.nodeSet[nodeID] {
		return // Node doesn't exist
	}

	delete(hr.nodeSet, nodeID)

	// Remove all virtual node hashes for this node
	newHashes := make([]uint32, 0, len(hr.sortedHashes))
	for _, hash := range hr.sortedHashes {
		if owner, exists := hr.hashToNode[hash]; exists && owner == nodeID {
			delete(hr.hashToNode, hash)
		} else {
			newHashes = append(newHashes, hash)
		}
	}
	hr.sortedHashes = newHashes
}

// Rebuild clears the ring and rebuilds it with the given list of nodes.
// Useful for cluster topology changes.
func (hr *HashRing) Rebuild(nodes []string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.sortedHashes = make([]uint32, 0)
	hr.hashToNode = make(map[uint32]string)
	hr.nodeSet = make(map[string]bool)

	for _, nodeID := range nodes {
		hr.nodeSet[nodeID] = true
		for i := 0; i < hr.virtualNodes; i++ {
			vNodeKey := fnv1aKey(nodeID, i)
			hash := fnv1a32(vNodeKey)
			hr.hashToNode[hash] = nodeID
			hr.sortedHashes = append(hr.sortedHashes, hash)
		}
	}

	sort.Slice(hr.sortedHashes, func(i, j int) bool {
		return hr.sortedHashes[i] < hr.sortedHashes[j]
	})
}

// GetOwner returns the node responsible for the given key.
// Uses binary search for O(log N) lookup.
func (hr *HashRing) GetOwner(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return ""
	}

	keyHash := fnv1a32(key)

	// Binary search for the first hash >= keyHash
	idx := sort.Search(len(hr.sortedHashes), func(i int) bool {
		return hr.sortedHashes[i] >= keyHash
	})

	// If no hash >= keyHash, wrap to first node
	if idx == len(hr.sortedHashes) {
		idx = 0
	}

	return hr.hashToNode[hr.sortedHashes[idx]]
}

// fnv1a32 computes FNV-1a 32-bit hash of the input string.
// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func fnv1a32(input string) uint32 {
	const (
		fnvOffset = uint32(2166136261)
		fnvPrime  = uint32(16777619)
	)

	hash := fnvOffset
	for i := 0; i < len(input); i++ {
		hash ^= uint32(input[i])
		hash *= fnvPrime
	}
	return hash
}

// fnv1aKey constructs a virtual node key from nodeID and index.
func fnv1aKey(nodeID string, index int) string {
	// Use a simple format: nodeID + "#" + index
	// Example: "node1#0", "node1#1", ..., "node1#49"
	result := make([]byte, 0, len(nodeID)+10)
	result = append(result, []byte(nodeID)...)
	result = append(result, '#')

	// Convert index to string (simple positive int conversion)
	if index == 0 {
		result = append(result, '0')
	} else {
		temp := index
		digits := make([]byte, 0)
		for temp > 0 {
			digits = append(digits, byte('0'+temp%10))
			temp /= 10
		}
		// Reverse digits
		for i := len(digits) - 1; i >= 0; i-- {
			result = append(result, digits[i])
		}
	}

	return string(result)
}
