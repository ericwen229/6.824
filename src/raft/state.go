package raft

import "time"

type Role int32

const (
	roleFollower  Role = 1
	roleCandidate Role = 2
	roleLeader    Role = 3
)

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
	rf.role = roleLeader
}
