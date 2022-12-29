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
	logEntries      []*LogEntry
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
	rf.votedFor = -1
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

func (rf *Raft) hasPrevLog(prevLogIndex, prevLogTerm int) bool {
	if prevLogIndex == 0 {
		return true
	}
	return rf.isLogIndexInRange(prevLogIndex) && rf.getEntry(prevLogIndex).Term == prevLogTerm
}

func (rf *Raft) appendCommand(command interface{}) {
	rf.logEntries = append(rf.logEntries, &LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
}

func (rf *Raft) appendLogEntry(entry *LogEntry) {
	rf.logEntries = append(rf.logEntries, entry)
}

func (rf *Raft) isMoreUpToDateThanMe(lastIndex, lastTerm int) bool {
	thisLastIndex := rf.getLastLogIndex()
	thisLastTerm := rf.getLastLogTerm()

	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer
	// is more up-to-date.
	if thisLastTerm != lastTerm {
		return lastTerm > thisLastTerm
	} else {
		return lastIndex >= thisLastIndex
	}
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
		rf.commitIndex = newCommitIndex
	}
}

func (rf *Raft) updateCommitIndex(newIndex int) {
	rf.commitIndex = newIndex
}

func (rf *Raft) removeEntriesFrom(startIndex int) {
	//   log index: 1 2 3 4 5
	// slice index: 0 1 2 3 4
	// after removeEntriesFrom(4)
	//   log index: 1 2 3
	// slice index: 0 1 2
	rf.logEntries = rf.logEntries[:startIndex-1]
}

func (rf *Raft) isLeader() bool {
	return rf.role == roleLeader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logEntries)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logEntries) == 0 {
		return NilTerm
	} else {
		return rf.logEntries[len(rf.logEntries)-1].Term
	}
}

func (rf *Raft) isLogIndexInRange(index int) bool {
	return index >= 1 && index <= rf.getLastLogIndex()
}

func (rf *Raft) isNilLogTerm(term int) bool {
	return term == NilTerm
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
	return rf.logEntries[index-1]
}

func (rf *Raft) getEntries(from int) []*LogEntry {
	src := rf.logEntries[from-1:]
	dst := make([]*LogEntry, len(src))
	copy(dst, src)
	return dst
}
