package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

type Role int32

const (
	roleFollower  Role = 1
	roleCandidate Role = 2
	roleLeader    Role = 3
)

type LogItem struct {
	command interface{}
	term    int
}

// Raft implements a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC endpoints of all peers
	persister *Persister          // object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm     int
	role            Role
	electionTimer   time.Time
	electionTimeout time.Duration
	votedFor        int
	log             []*LogItem
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
}

// GetState returns current term and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == roleLeader
}

// unprotected state transfer methods

func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.role = roleFollower
	rf.electionTimer = time.Now()
	rf.electionTimeout = randElectionTimeout()
	rf.votedFor = -1
}

func (rf *Raft) initFollower() {
	rf.role = roleFollower
	rf.electionTimer = time.Now()
	rf.electionTimeout = randElectionTimeout()
	rf.votedFor = -1
}

func (rf *Raft) initCandidate() {
	rf.currentTerm++
	rf.role = roleCandidate
	rf.electionTimer = time.Now()
	rf.electionTimeout = randElectionTimeout()
	rf.votedFor = rf.me
}

func (rf *Raft) convertToLeader() {
	peerNum := len(rf.peers)

	rf.role = roleLeader
	rf.nextIndex = make([]int, peerNum)
	for i := 0; i < peerNum; i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, peerNum)
}

func (rf *Raft) appendLogItem(command any) {
	rf.log = append(rf.log, &LogItem{
		command: command,
		term:    rf.currentTerm,
	})
}

func (rf *Raft) isLeader() bool {
	return rf.role == roleLeader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log)
}
