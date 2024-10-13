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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	TICK_INTERVAL_MS        int64 = 5
	ELECTION_TIMEOUT_MAX_MS int64 = 1000
	ELECTION_TIMEOUT_MIN_MS int64 = 500
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states
	currentTerm int
	votedFor    int

	// Volatile states
	role            Role
	electionTimeout *Countdown
}

func (rf *Raft) initFollower() {
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.electionTimeout = NewCountdown(newRandElectionTimeout())
}

func (rf *Raft) follower2Candidate() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.role = Candidate
	rf.electionTimeout = NewCountdown(newRandElectionTimeout())
}

func (rf *Raft) foundHigherTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = Follower
	rf.electionTimeout = NewCountdown(newRandElectionTimeout())
}

func (rf *Raft) becomeLeader() {
	// rf.currentTerm not changed
	rf.votedFor = -1
	rf.role = Leader
	rf.electionTimeout = nil
}

func newRandElectionTimeout() int64 {
	var max = ELECTION_TIMEOUT_MAX_MS
	var min = ELECTION_TIMEOUT_MIN_MS
	return rand.Int63n(max-min) + min
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.initFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine
	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	var tickIntervalMs int64 = TICK_INTERVAL_MS
	for rf.killed() == false {
		time.Sleep(time.Duration(tickIntervalMs) * time.Millisecond)
		rf.tick(tickIntervalMs)
	}
}

func (rf *Raft) tick(elapsedMs int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isFollower() {
		// if election timeout elapses without receiving AppendEntries RPC
		// from current leader or granting vote to candidate, convert to candidate
		if rf.electionTimeout.Tick(elapsedMs) {
			rf.follower2Candidate()
			rf.requestVotesFromPeers()
		}
	} else if rf.isCandidate() {

	} else if rf.isFollower() {

	} else {
		panic(fmt.Errorf("illegal raft role: %v", rf.role))
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
