package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft/util"
)

func (rf *Raft) isFollower() bool {
	return rf.role == follower
}

func (rf *Raft) isCandidate() bool {
	return rf.role == candidate
}

func (rf *Raft) isLeader() bool {
	return rf.role == leader
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *util.Persister     // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states
	currentTerm int
	votedFor    int
	logs        *LogEntries

	// Volatile states
	role             role
	electionTimeout  *util.Countdown // non leader only
	heartbeatTimeout *util.Countdown // leader only
	commitIndex      int
	lastApplied      int
	nextIndex        []int // leader only
	matchIndex       []int // leader only
}

func (rf *Raft) initFollower() {
	rf.logState("-> follower 0")

	rf.currentTerm = zeroTerm
	rf.votedFor = votedForNoOne
	rf.logs = newEntries()
	rf.role = follower
	rf.electionTimeout = util.NewCountdown(randElectionTimeout())
	rf.heartbeatTimeout = nil
	rf.commitIndex = zeroIndex
	rf.lastApplied = zeroIndex
	rf.nextIndex = nil
	rf.matchIndex = nil
}

func (rf *Raft) follower2Candidate() {
	// on conversion to candidate, start election:
	// - increment currentTerm
	// - vote for self
	// - reset election timer
	// - send RequestVote RPCs to all other servers

	rf.logState("follower %d -> candidate %d", rf.currentTerm, rf.currentTerm+1)

	rf.currentTerm++
	rf.votedFor = rf.me
	// rf.logs not changed
	rf.role = candidate
	rf.electionTimeout = util.NewCountdown(randElectionTimeout())
	// rf.heartbeatTimeout not changed
	// rf.commitIndex not changed
	// rf.lastApplied not changed
	// rf.nextIndex not changed
	// rf.matchIndex not changed

	rf.startElection()
}

func (rf *Raft) candidate2Follower() {
	rf.logState("candidate %d -> follower %d", rf.currentTerm, rf.currentTerm)

	// rf.currentTerm not changed
	// rf.votedFor not changed
	// rf.logs not changed
	rf.role = follower
	// rf.electionTimeout not changed
	// rf.heartbeatTimeout not changed
	// rf.commitIndex not changed
	// rf.lastApplied not changed
	// rf.nextIndex not changed
	// rf.matchIndex not changed

}

func (rf *Raft) candidateRetryElection() {
	rf.logState("candidate %d -> candidate %d", rf.currentTerm, rf.currentTerm+1)

	rf.currentTerm++
	// rf.votedFor not changed (still self)
	// rf.logs not changed
	// rf.role not changed (still candidate)
	rf.electionTimeout = util.NewCountdown(randElectionTimeout())
	// rf.heartbeatTimeout not changed
	// rf.commitIndex not changed
	// rf.lastApplied not changed
	// rf.nextIndex not changed
	// rf.matchIndex not changed

	rf.startElection()
}

func (rf *Raft) foundHigherTerm(term int) {
	rf.logState("%s %d -> follower %d", rf.role, rf.currentTerm, term)

	rf.currentTerm = term
	rf.votedFor = votedForNoOne
	// rf.logs not changed
	rf.role = follower
	if rf.electionTimeout == nil { // use to be leader
		rf.electionTimeout = util.NewCountdown(randElectionTimeout())
	}
	rf.heartbeatTimeout = nil
	// rf.commitIndex not changed
	// rf.lastApplied not changed
	rf.nextIndex = nil
	rf.matchIndex = nil
}

func (rf *Raft) candidate2Leader() {
	rf.logState("candidate %d -> leader %d", rf.currentTerm, rf.currentTerm)

	// rf.currentTerm not changed
	// rf.votedFor not changed
	// rf.logs not changed
	rf.role = leader
	rf.electionTimeout = nil
	rf.heartbeatTimeout = util.NewCountdown(heartbeatTimeout())
	// rf.commitIndex not changed
	// rf.lastApplied not changed
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.logs.lastIndex() + 1
		}
	}
	rf.matchIndex = make([]int, len(rf.peers))

	// upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
	rf.broadcastHeartbeat()
}

func (rf *Raft) appendEntryLocal(command interface{}) (int, int) {
	return rf.logs.append(&LogEntry{Term: rf.currentTerm, Command: command}), rf.currentTerm
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout.Reset()
}

func (rf *Raft) resetHeartbeatTimeout() {
	rf.heartbeatTimeout.Reset()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	electionTimeoutMaxMs int64 = 500
	electionTimeoutMinMs int64 = 300
	heartbeatTimeoutMs   int64 = 100
)

func randElectionTimeout() int64 {
	var max = electionTimeoutMaxMs
	var min = electionTimeoutMinMs
	return rand.Int63n(max-min) + min
}

func heartbeatTimeout() int64 {
	return heartbeatTimeoutMs
}
