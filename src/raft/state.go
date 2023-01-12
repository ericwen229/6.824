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

// Raft implements a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC endpoints of all peers
	persister       *Persister          // object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	currentTerm     int
	role            Role
	electionTimer   time.Time
	electionTimeout time.Duration
	votedFor        int
	snapshot        *Snapshot
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

func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.role = roleFollower
	rf.votedFor = -1

	// persistence
	rf.save()
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

func (rf *Raft) updateCommitIndex() {
	// collect matchIndices
	// for leader himself, matchIndex is its log length
	var matchIndexList []int
	for i, index := range rf.matchIndex {
		if i != rf.me {
			matchIndexList = append(matchIndexList, index)
		} else {
			matchIndexList = append(matchIndexList, rf.getLastLogIndex())
		}
	}

	// sort desc
	sort.Slice(matchIndexList, func(i, j int) bool {
		return matchIndexList[i] > matchIndexList[j]
	})

	// matchIndexList: [9 6 3] 2 1
	// latestCommitIndex:   ^
	peerNum := len(rf.peers)
	majorityNum := peerNum/2 + 1
	latestCommitIndex := matchIndexList[majorityNum-1]

	// Leader Rule 4:
	// if there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] >= N,
	// and log[N].term == currentTerm
	// set commitIndex = N
	if latestCommitIndex > rf.commitIndex && rf.getEntry(latestCommitIndex).Term == rf.currentTerm {
		rf.commitIndex = latestCommitIndex
	}
}
