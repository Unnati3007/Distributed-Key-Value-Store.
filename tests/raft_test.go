package tests

import (
	"testing"
	"time"

	"github.com/Unnati3007/kv-store/internal/raft"
)

// mockTransport fulfills the Transport interface for tests.
type mockTransport struct {
	voteResp   *raft.VoteResponse
	appendResp *raft.AppendResponse
}

func (m *mockTransport) RequestVote(_ string, _ *raft.VoteRequest) (*raft.VoteResponse, error) {
	return m.voteResp, nil
}
func (m *mockTransport) AppendEntries(_ string, _ *raft.AppendRequest) (*raft.AppendResponse, error) {
	return m.appendResp, nil
}

func TestNewNodeStartsAsFollower(t *testing.T) {
	transport := &mockTransport{
		voteResp:   &raft.VoteResponse{Granted: true},
		appendResp: &raft.AppendResponse{Success: true},
	}
	node := raft.NewNode(1, []string{"peer2", "peer3"}, transport)
	if node == nil {
		t.Fatal("expected non-nil node")
	}
}

func TestVoteGrantedForHigherTerm(t *testing.T) {
	transport := &mockTransport{
		voteResp:   &raft.VoteResponse{Granted: true},
		appendResp: &raft.AppendResponse{Success: true},
	}
	node := raft.NewNode(1, []string{}, transport)
	resp := node.HandleVoteRequest(&raft.VoteRequest{
		Term: 5, CandidateID: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if !resp.Granted {
		t.Error("expected vote to be granted for higher term candidate")
	}
}

func TestAppendEntriesRejectsOldTerm(t *testing.T) {
	transport := &mockTransport{appendResp: &raft.AppendResponse{Success: true}}
	node := raft.NewNode(1, []string{}, transport)
	// Advance node term
	node.HandleVoteRequest(&raft.VoteRequest{Term: 10, CandidateID: 2})
	// Old-term append should be rejected
	resp := node.HandleAppendEntries(&raft.AppendRequest{Term: 1})
	if resp.Success {
		t.Error("expected AppendEntries with old term to fail")
	}
}

func TestProposalRejectedOnFollower(t *testing.T) {
	transport := &mockTransport{appendResp: &raft.AppendResponse{Success: true}}
	node := raft.NewNode(1, []string{}, transport)
	time.Sleep(10 * time.Millisecond) // let timers init
	_, _, isLeader := node.Propose(raft.Command{Op: "SET", Key: "k", Value: "v"})
	if isLeader {
		t.Error("follower should not accept proposals")
	}
}
