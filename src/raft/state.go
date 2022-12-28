package raft

import (
	"6.824/labrpc"
	"sort"
	"sync"
	"time"
)

type Role int32

const (
	roleFollower  Role = 1
	roleCandidate Role = 2
	roleLeader    Role = 3
)

type LogEntry struct {
	Command interface{}
	Term    int
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
	log             []*LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	commitCond      *sync.Cond
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
	rf.commitCond = sync.NewCond(&rf.mu)
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

func (rf *Raft) appendCommand(command interface{}) {
	rf.log = append(rf.log, &LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
}

func (rf *Raft) appendLogEntry(entry *LogEntry) {
	rf.log = append(rf.log, entry)
}

func (rf *Raft) updateLogEntries(startIndex int, entries []*LogEntry) {
	for i, entry := range entries {
		thisIndex := startIndex + i
		if rf.isLogIndexInRange(thisIndex) {
			if rf.getEntry(thisIndex).Term != entry.Term {
				rf.removeEntriesFrom(thisIndex)
				rf.appendLogEntry(entry)
			}
		} else {
			rf.appendLogEntry(entry)
		}
	}
}

func (rf *Raft) isMoreUpToDate(lastIndex, lastTerm int) bool {
	thisLastIndex := rf.getLastLogIndex()
	thisLastTerm := rf.getLastLogTerm()

	if !rf.isNonNilLogTerm(thisLastTerm) {
		return true
	} else if !rf.isNonNilLogTerm(lastTerm) {
		return false
	}

	return lastIndex >= thisLastIndex
}

func (rf *Raft) updateCommitStatus() {
	var matchIndexList []int
	for i, index := range rf.matchIndex {
		if i != rf.me {
			matchIndexList = append(matchIndexList, index)
		}
	}
	sort.Slice(matchIndexList, func(i, j int) bool {
		return matchIndexList[i] > matchIndexList[j]
	})
	peerNum := len(rf.peers)
	majorityNum := peerNum/2 + 1
	newCommitIndex := matchIndexList[majorityNum-1-1]
	if newCommitIndex > rf.commitIndex && rf.getEntry(newCommitIndex).Term == rf.currentTerm {
		rf.updateCommitIndex(newCommitIndex)
	}
}

func (rf *Raft) updateCommitIndex(newIndex int) {
	if newIndex < rf.commitIndex {
		panic("commit index decreases")
	} else if newIndex == rf.commitIndex {
		return
	}
	rf.commitIndex = newIndex
	rf.commitCond.Broadcast()
}

func (rf *Raft) removeEntriesFrom(startIndex int) {
	//   log index: 1 2 3 4 5
	// slice index: 0 1 2 3 4
	rf.log = rf.log[:startIndex-1]
}

func (rf *Raft) isLeader() bool {
	return rf.role == roleLeader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return NilTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) isLogIndexInRange(index int) bool {
	return index >= 1 && index <= rf.getLastLogIndex()
}

func (rf *Raft) isNilLogIndex(index int) bool {
	return index == NilIndex
}

func (rf *Raft) isNonNilLogTerm(term int) bool {
	return term != NilTerm
}

func (rf *Raft) getEntriesToSend(nextIndex int) []*LogEntry {
	if rf.isLogIndexInRange(nextIndex) {
		return rf.getEntries(nextIndex)
	} else {
		return nil
	}
}

func (rf *Raft) getEntry(index int) *LogEntry {
	return rf.log[index-1]
}

func (rf *Raft) getEntries(from int) []*LogEntry {
	return rf.log[from-1:]
}
