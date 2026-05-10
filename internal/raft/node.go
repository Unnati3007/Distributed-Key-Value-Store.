package raft

import (
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	HeartbeatInterval  = 50 * time.Millisecond
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
)

// Command represents a KV operation to be applied to the state machine.
type Command struct {
	Op    string // "SET" | "DEL"
	Key   string
	Value string
}

// LogEntry is one entry in the Raft log.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command Command
}

// Node is a single Raft participant.
type Node struct {
	mu sync.Mutex

	id    uint32
	peers []string // gRPC addresses of peer nodes

	// Persistent state (must survive restarts)
	currentTerm uint64
	votedFor    int32 // -1 if none
	log         []LogEntry

	// Volatile state
	state       State
	commitIndex uint64
	lastApplied uint64

	// Leader-only volatile state
	nextIndex  map[uint32]uint64 // next log index to send to each peer
	matchIndex map[uint32]uint64 // highest log index known to be replicated on each peer

	// Channels
	applyCh     chan LogEntry // committed entries sent here for state machine to apply
	electionTimer *time.Timer

	// Transport (injected)
	transport Transport
}

// Transport abstracts the network layer so it can be mocked in tests.
type Transport interface {
	RequestVote(peer string, req *VoteRequest) (*VoteResponse, error)
	AppendEntries(peer string, req *AppendRequest) (*AppendResponse, error)
}

type VoteRequest struct {
	Term         uint64
	CandidateID  uint32
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term    uint64
	Granted bool
}

type AppendRequest struct {
	Term         uint64
	LeaderID     uint32
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendResponse struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
}

// NewNode creates a new Raft node.
func NewNode(id uint32, peers []string, transport Transport) *Node {
	n := &Node{
		id:        id,
		peers:     peers,
		votedFor:  -1,
		state:     Follower,
		nextIndex: make(map[uint32]uint64),
		matchIndex: make(map[uint32]uint64),
		applyCh:   make(chan LogEntry, 256),
		transport: transport,
	}
	n.resetElectionTimer()
	return n
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := ElectionTimeoutMin + time.Duration(
		float64(ElectionTimeoutMax-ElectionTimeoutMin)*pseudoRand(),
	)
	n.electionTimer = time.AfterFunc(timeout, n.startElection)
}

func pseudoRand() float64 {
	return float64(time.Now().UnixNano()%1000) / 1000.0
}

// Propose submits a command to the Raft cluster (leader only).
// Returns the log index and term, or an error if not the leader.
func (n *Node) Propose(cmd Command) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, 0, false
	}
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log)) + 1,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	// Trigger replication (simplified — real impl uses dedicated goroutines per peer)
	go n.replicateLog()
	return entry.Index, entry.Term, true
}

func (n *Node) replicateLog() {
	// In the full implementation this sends AppendEntries RPCs to all peers
	// and advances commitIndex once a majority acknowledges.
}

// HandleVoteRequest processes an incoming RequestVote RPC.
func (n *Node) HandleVoteRequest(req *VoteRequest) *VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = -1
	}
	lastIdx := uint64(len(n.log))
	lastTerm := uint64(0)
	if lastIdx > 0 {
		lastTerm = n.log[lastIdx-1].Term
	}
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if req.Term >= n.currentTerm && upToDate &&
		(n.votedFor == -1 || n.votedFor == int32(req.CandidateID)) {
		n.votedFor = int32(req.CandidateID)
		n.resetElectionTimer()
		return &VoteResponse{Term: n.currentTerm, Granted: true}
	}
	return &VoteResponse{Term: n.currentTerm, Granted: false}
}

// HandleAppendEntries processes an incoming AppendEntries (heartbeat) RPC.
func (n *Node) HandleAppendEntries(req *AppendRequest) *AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		return &AppendResponse{Term: n.currentTerm, Success: false}
	}
	n.currentTerm = req.Term
	n.state = Follower
	n.resetElectionTimer()

	// Consistency check
	if req.PrevLogIndex > 0 {
		if uint64(len(n.log)) < req.PrevLogIndex ||
			n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			return &AppendResponse{Term: n.currentTerm, Success: false}
		}
	}

	// Append new entries
	n.log = append(n.log[:req.PrevLogIndex], req.Entries...)

	// Advance commit index
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min64(req.LeaderCommit, uint64(len(n.log)))
		go n.applyCommitted()
	}
	return &AppendResponse{Term: n.currentTerm, Success: true, MatchIndex: uint64(len(n.log))}
}

func (n *Node) applyCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		n.applyCh <- n.log[n.lastApplied-1]
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = int32(n.id)
	term := n.currentTerm
	lastIdx := uint64(len(n.log))
	lastTerm := uint64(0)
	if lastIdx > 0 {
		lastTerm = n.log[lastIdx-1].Term
	}
	peers := n.peers
	n.mu.Unlock()

	votes := 1
	majority := (len(peers)+2)/2 + 1
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			resp, err := n.transport.RequestVote(p, &VoteRequest{
				Term: term, CandidateID: n.id,
				LastLogIndex: lastIdx, LastLogTerm: lastTerm,
			})
			if err != nil || !resp.Granted {
				return
			}
			mu.Lock()
			votes++
			if votes >= majority {
				n.becomeLeader()
			}
			mu.Unlock()
		}(peer)
	}
	wg.Wait()
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Candidate {
		return
	}
	n.state = Leader
	for _, p := range n.peers {
		// Simple peer ID extraction — real impl maps addr → id
		_ = p
	}
	go n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		for _, peer := range n.peers {
			go func(p string) {
				n.mu.Lock()
				req := &AppendRequest{
					Term: n.currentTerm, LeaderID: n.id,
					LeaderCommit: n.commitIndex,
				}
				n.mu.Unlock()
				n.transport.AppendEntries(p, req) //nolint
			}(peer)
		}
	}
}

func min64(a, b uint64) uint64 {
	if a < b { return a }
	return b
}
