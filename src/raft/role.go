package raft

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (rf *Raft) isFollower() bool {
	return rf.role == Follower
}

func (rf *Raft) isCandidate() bool {
	return rf.role == Candidate
}

func (rf *Raft) isLeader() bool {
	return rf.role == Leader
}
