package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"shardscale/internal/metrics"
	"shardscale/internal/store"
)

// NodeState represents the state of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term  int
	Key   string
	Value []byte
	Index int
}

// RaftNode represents a Raft consensus node.
type RaftNode struct {
	// Persistent state on all servers
	mu          sync.Mutex
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]int
	matchIndex map[string]int

	// Node identity and state
	id       string
	state    NodeState
	leaderId string
	peers    []string
	peerAddr map[string]string // peer ID -> address

	// Storage and metrics
	store   *store.Store
	metrics *metrics.Metrics
	logger  *slog.Logger

	// Timers
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Channels for coordination
	electionChan  chan struct{}
	heartbeatChan chan struct{}
	applyChan     chan struct{}
	stopChan      chan struct{}
	stoppedChan   chan struct{}

	// Client RPC handling
	httpClient *http.Client

	// Configuration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
}

// New creates a new Raft node.
func New(id string, peers []string, peerAddr map[string]string, s *store.Store, m *metrics.Metrics, logger *slog.Logger) *RaftNode {
	rn := &RaftNode{
		id:                 id,
		state:              Follower,
		peers:              peers,
		peerAddr:           peerAddr,
		store:              s,
		metrics:            m,
		logger:             logger,
		log:                make([]LogEntry, 0),
		nextIndex:          make(map[string]int),
		matchIndex:         make(map[string]int),
		votedFor:           "",
		leaderId:           "",
		commitIndex:        -1,
		lastApplied:        -1,
		currentTerm:        0,
		electionChan:       make(chan struct{}, 1),
		heartbeatChan:      make(chan struct{}, 1),
		applyChan:          make(chan struct{}, 1),
		stopChan:           make(chan struct{}),
		stoppedChan:        make(chan struct{}),
		electionTimeoutMin: 150 * time.Millisecond,
		electionTimeoutMax: 300 * time.Millisecond,
		heartbeatInterval:  50 * time.Millisecond,
		httpClient: &http.Client{
			Timeout: 1 * time.Second,
		},
	}

	// Initialize nextIndex and matchIndex for peers
	for _, peer := range peers {
		rn.nextIndex[peer] = 0
		rn.matchIndex[peer] = -1
	}

	return rn
}

// Start begins the Raft node's main event loops.
func (rn *RaftNode) Start() {
	go rn.run()
}

// Stop gracefully shuts down the Raft node.
func (rn *RaftNode) Stop() {
	close(rn.stopChan)
	<-rn.stoppedChan
	rn.logger.Info("raft node stopped", slog.String("id", rn.id))
}

// run is the main event loop for the Raft node.
func (rn *RaftNode) run() {
	defer close(rn.stoppedChan)

	rn.resetElectionTimer()

	// Ticker for periodic metrics updates
	metricsTicker := time.NewTicker(1 * time.Second)
	defer metricsTicker.Stop()

	for {
		select {
		case <-rn.stopChan:
			rn.logger.Info("raft node stopping", slog.String("id", rn.id))
			if rn.electionTimer != nil {
				rn.electionTimer.Stop()
			}
			if rn.heartbeatTicker != nil {
				rn.heartbeatTicker.Stop()
			}
			return

		case <-rn.electionTimer.C:
			// Election timeout triggered
			rn.mu.Lock()
			if rn.state != Leader {
				rn.becomeCandidate()
			}
			rn.mu.Unlock()

		case <-rn.heartbeatChan:
			// Heartbeat triggered (leaders only)
			rn.mu.Lock()
			if rn.state == Leader {
				rn.sendHeartbeats()
			}
			rn.mu.Unlock()

		case <-rn.applyChan:
			// Apply committed entries to state machine
			rn.applyCommittedEntries()

		case <-metricsTicker.C:
			// Update metrics
			rn.mu.Lock()
			if rn.metrics != nil {
				rn.metrics.SetRaftCurrentTerm(int64(rn.currentTerm))
				rn.metrics.SetRaftCommitIndex(int64(rn.commitIndex))
				rn.metrics.SetRaftLogLength(int64(len(rn.log)))
			}
			rn.mu.Unlock()
		}
	}
}

// resetElectionTimer resets the election timer with a randomized timeout.
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	timeout := rn.electionTimeoutMin + time.Duration(rand.Intn(int(rn.electionTimeoutMax-rn.electionTimeoutMin)))
	rn.electionTimer = time.AfterFunc(timeout, func() {
		select {
		case rn.electionChan <- struct{}{}:
		default:
		}
	})
}

// becomeCandidate transitions node to Candidate state and starts election.
func (rn *RaftNode) becomeCandidate() {
	if rn.state == Leader {
		return
	}

	rn.currentTerm++
	rn.state = Candidate
	rn.votedFor = rn.id
	rn.leaderId = ""
	rn.resetElectionTimer()

	rn.logger.Info("node became candidate",
		slog.String("id", rn.id),
		slog.Int("term", rn.currentTerm),
	)

	if rn.metrics != nil {
		// Metrics update deferred to GetState() for atomicity
	}

	// Request votes from all peers
	go rn.requestVotes()
}

// becomeLeader transitions node to Leader state and starts heartbeat.
func (rn *RaftNode) becomeLeader() {
	rn.state = Leader
	rn.leaderId = rn.id
	rn.votedFor = ""

	// Initialize nextIndex and matchIndex for all peers
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = len(rn.log)
		rn.matchIndex[peer] = -1
	}

	rn.logger.Info("node became leader",
		slog.String("id", rn.id),
		slog.Int("term", rn.currentTerm),
	)

	if rn.metrics != nil {
		// Metrics update for leader election
	}

	// Stop previous heartbeat ticker if any
	if rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
	}

	// Start heartbeat ticker
	rn.heartbeatTicker = time.NewTicker(rn.heartbeatInterval)
	go func() {
		for {
			select {
			case <-rn.stopChan:
				return
			case <-rn.heartbeatTicker.C:
				rn.mu.Lock()
				if rn.state == Leader {
					rn.sendHeartbeats()
				}
				rn.mu.Unlock()
			}
		}
	}()

	// Send initial heartbeats
	rn.sendHeartbeats()
}

// requestVotes sends RequestVote RPCs to all peers.
func (rn *RaftNode) requestVotes() {
	rn.mu.Lock()
	currentTerm := rn.currentTerm
	candidateID := rn.id
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rn.log[lastLogIndex].Term
	}
	rn.mu.Unlock()

	voteCount := 1 // Vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range rn.peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()

			req := &RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			resp := &RequestVoteResponse{}
			if !rn.callRequestVote(peerID, req, resp) {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// If term is higher, convert to follower
			if resp.Term > rn.currentTerm {
				rn.currentTerm = resp.Term
				rn.state = Follower
				rn.votedFor = ""
				rn.leaderId = ""
				rn.resetElectionTimer()
				return
			}

			if resp.VoteGranted && rn.state == Candidate {
				voteMu.Lock()
				voteCount++
				voteMu.Unlock()

				// Check if we have majority
				if voteCount > len(rn.peers)/2 {
					rn.becomeLeader()
				}
			}
		}(peer)
	}

	wg.Wait()
}

// sendHeartbeats sends AppendEntries RPCs to all peers (for leaders only).
func (rn *RaftNode) sendHeartbeats() {
	currentTerm := rn.currentTerm
	leaderID := rn.id
	commitIndex := rn.commitIndex

	for _, peer := range rn.peers {
		go func(peerID string) {
			rn.mu.Lock()
			prevLogIndex := rn.nextIndex[peerID] - 1
			prevLogTerm := 0
			if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
				prevLogTerm = rn.log[prevLogIndex].Term
			}

			entries := make([]LogEntry, 0)
			if rn.nextIndex[peerID] < len(rn.log) {
				entries = append(entries, rn.log[rn.nextIndex[peerID]:]...)
			}
			rn.mu.Unlock()

			req := &AppendEntriesRequest{
				Term:         currentTerm,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}

			resp := &AppendEntriesResponse{}
			if !rn.callAppendEntries(peerID, req, resp) {
				// Network error - retry later
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// If term is higher, convert to follower
			if resp.Term > rn.currentTerm {
				rn.currentTerm = resp.Term
				rn.state = Follower
				rn.votedFor = ""
				rn.leaderId = ""
				rn.resetElectionTimer()
				return
			}

			if rn.state != Leader {
				return
			}

			if resp.Success {
				// Update nextIndex and matchIndex
				rn.nextIndex[peerID] = prevLogIndex + 1 + len(entries)
				rn.matchIndex[peerID] = prevLogIndex + len(entries)

				// Update commitIndex if appropriate
				rn.updateCommitIndex()
			} else {
				// Log mismatch - decrement nextIndex
				if rn.nextIndex[peerID] > 0 {
					rn.nextIndex[peerID]--
				}
			}
		}(peer)
	}
}

// updateCommitIndex updates commitIndex based on replication status.
func (rn *RaftNode) updateCommitIndex() {
	matchIndices := make([]int, len(rn.peers))
	for i, peer := range rn.peers {
		matchIndices[i] = rn.matchIndex[peer]
	}

	// Add self's match index
	selfMatches := append(matchIndices, len(rn.log)-1)

	// Find the median match index
	for i := len(selfMatches) - 1; i >= 0; i-- {
		count := 0
		for _, idx := range selfMatches {
			if idx >= i {
				count++
			}
		}

		if count > len(rn.peers)/2 && i > rn.commitIndex && i < len(rn.log) {
			if rn.log[i].Term == rn.currentTerm {
				rn.commitIndex = i

				// Trigger apply
				select {
				case rn.applyChan <- struct{}{}:
				default:
				}
			}
			break
		}
	}
}

// applyCommittedEntries applies committed entries to the state machine.
func (rn *RaftNode) applyCommittedEntries() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		if rn.lastApplied < len(rn.log) {
			entry := rn.log[rn.lastApplied]
			rn.logger.Debug("applying log entry",
				slog.String("id", rn.id),
				slog.Int("index", rn.lastApplied),
				slog.Int("term", entry.Term),
				slog.String("key", entry.Key),
			)

			// Apply to store
			ctx := context.Background()
			if err := rn.store.Put(ctx, entry.Key, string(entry.Value)); err != nil {
				rn.logger.Error("failed to apply log entry",
					slog.String("id", rn.id),
					slog.Int("index", rn.lastApplied),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

// AppendEntry appends a new entry to the log (called by leader).
// Returns whether the entry was appended.
func (rn *RaftNode) AppendEntry(key string, value []byte) (bool, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return false, fmt.Errorf("not leader")
	}

	entry := LogEntry{
		Term:  rn.currentTerm,
		Key:   key,
		Value: value,
		Index: len(rn.log),
	}

	rn.log = append(rn.log, entry)

	rn.logger.Debug("appended log entry",
		slog.String("id", rn.id),
		slog.Int("index", entry.Index),
		slog.Int("term", entry.Term),
		slog.String("key", key),
	)

	return true, nil
}

// WaitForReplication waits for an entry to be replicated to a quorum.
// Returns true if replicated, false if timeout/not leader.
func (rn *RaftNode) WaitForReplication(index int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return false
		}

		// Count replications
		replicatedCount := 1 // Self
		for _, matchIdx := range rn.matchIndex {
			if matchIdx >= index {
				replicatedCount++
			}
		}

		if replicatedCount > len(rn.peers)/2 {
			rn.mu.Unlock()
			return true
		}

		rn.mu.Unlock()

		if time.Now().After(deadline) {
			return false
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// RequestVoteHandler handles a RequestVote RPC.
func (rn *RaftNode) RequestVoteHandler(req *RequestVoteRequest, resp *RequestVoteResponse) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp.Term = rn.currentTerm

	// If term is higher, update and become follower
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = Follower
		rn.leaderId = ""
		rn.resetElectionTimer()
	}

	resp.VoteGranted = false

	// Can only grant vote if terms match and haven't voted yet
	if req.Term == rn.currentTerm && (rn.votedFor == "" || rn.votedFor == req.CandidateID) {
		// Check if candidate's log is at least as up-to-date as ours
		lastLogIndex := len(rn.log) - 1
		lastLogTerm := 0
		if lastLogIndex >= 0 {
			lastLogTerm = rn.log[lastLogIndex].Term
		}

		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			rn.votedFor = req.CandidateID
			resp.VoteGranted = true
			rn.resetElectionTimer()

			rn.logger.Debug("vote granted",
				slog.String("id", rn.id),
				slog.String("candidate", req.CandidateID),
				slog.Int("term", req.Term),
			)
		}
	}
}

// AppendEntriesHandler handles an AppendEntries RPC.
func (rn *RaftNode) AppendEntriesHandler(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp.Term = rn.currentTerm

	// If term is higher, update and become follower
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = Follower
		rn.leaderId = req.LeaderID
		rn.resetElectionTimer()
	}

	resp.Success = false

	// Only accept from current term with matching term
	if req.Term < rn.currentTerm {
		return
	}

	// Update leader ID
	if req.Term == rn.currentTerm {
		rn.leaderId = req.LeaderID
		rn.state = Follower
		rn.resetElectionTimer()
	}

	// Check if we have the previous log entry
	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= len(rn.log) {
			// Log too short
			return
		}

		if rn.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			// Log mismatch
			return
		}
	}

	// Append new entries
	newLastLogIndex := req.PrevLogIndex
	for _, entry := range req.Entries {
		newLastLogIndex++
		if newLastLogIndex < len(rn.log) {
			// Check for conflict
			if rn.log[newLastLogIndex].Term != entry.Term {
				// Truncate log
				rn.log = rn.log[:newLastLogIndex]
				rn.log = append(rn.log, entry)
			}
		} else {
			rn.log = append(rn.log, entry)
		}
	}

	// Update commitIndex
	if req.LeaderCommit > rn.commitIndex {
		oldCommitIndex := rn.commitIndex
		rn.commitIndex = req.LeaderCommit
		if rn.commitIndex >= len(rn.log) {
			rn.commitIndex = len(rn.log) - 1
		}

		if rn.commitIndex > oldCommitIndex {
			// Trigger apply
			select {
			case rn.applyChan <- struct{}{}:
			default:
			}
		}
	}

	resp.Success = true
}

// callRequestVote sends a RequestVote RPC to a peer.
func (rn *RaftNode) callRequestVote(peerID string, req *RequestVoteRequest, resp *RequestVoteResponse) bool {
	// Get peer address
	peerAddr, ok := rn.peerAddr[peerID]
	if !ok {
		rn.logger.Debug("peer address not found",
			slog.String("peer", peerID),
		)
		return false
	}

	reqBody, _ := json.Marshal(req)
	httpReq, err := http.NewRequest("POST", fmt.Sprintf("http://%s/raft/requestVote", peerAddr),
		bytes.NewBuffer(reqBody))
	if err != nil {
		rn.logger.Debug("failed to create request",
			slog.String("peer", peerID),
			slog.String("error", err.Error()),
		)
		return false
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := rn.httpClient.Do(httpReq)
	if err != nil {
		rn.logger.Debug("rpc call failed",
			slog.String("peer", peerID),
			slog.String("error", err.Error()),
		)
		return false
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		rn.logger.Debug("rpc failed with status",
			slog.String("peer", peerID),
			slog.Int("status", httpResp.StatusCode),
		)
		return false
	}

	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		rn.logger.Debug("failed to decode response",
			slog.String("peer", peerID),
			slog.String("error", err.Error()),
		)
		return false
	}

	return true
}

// callAppendEntries sends an AppendEntries RPC to a peer.
func (rn *RaftNode) callAppendEntries(peerID string, req *AppendEntriesRequest, resp *AppendEntriesResponse) bool {
	// Get peer address
	peerAddr, ok := rn.peerAddr[peerID]
	if !ok {
		rn.logger.Debug("peer address not found",
			slog.String("peer", peerID),
		)
		return false
	}

	reqBody, _ := json.Marshal(req)
	httpReq, err := http.NewRequest("POST", fmt.Sprintf("http://%s/raft/appendEntries", peerAddr),
		bytes.NewBuffer(reqBody))
	if err != nil {
		rn.logger.Debug("failed to create request",
			slog.String("peer", peerID),
			slog.String("error", err.Error()),
		)
		return false
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := rn.httpClient.Do(httpReq)
	if err != nil {
		rn.logger.Debug("rpc call failed",
			slog.String("peer", peerID),
			slog.String("error", err.Error()),
		)
		return false
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		rn.logger.Debug("rpc failed with status",
			slog.String("peer", peerID),
			slog.Int("status", httpResp.StatusCode),
		)
		return false
	}

	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		rn.logger.Debug("failed to decode response",
			slog.String("peer", peerID),
			slog.String("error", err.Error()),
		)
		return false
	}

	return true
}

// GetState returns the current state of the node.
func (rn *RaftNode) GetState() (state NodeState, term int, isLeader bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state, rn.currentTerm, rn.state == Leader
}

// GetLeaderID returns the current leader ID.
func (rn *RaftNode) GetLeaderID() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.leaderId
}

// GetLogLength returns the number of entries in the log.
func (rn *RaftNode) GetLogLength() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return len(rn.log)
}

// GetCommitIndex returns the current commit index.
func (rn *RaftNode) GetCommitIndex() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.commitIndex
}
