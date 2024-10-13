package raft

import (
	"fmt"
	"time"
)

const TickIntervalMs int64 = 5

func (rf *Raft) ticker() {
	var tickIntervalMs int64 = TickIntervalMs
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
		}
	} else if rf.isCandidate() {
		// if election timeout elapses: start new election
		if rf.electionTimeout.Tick(elapsedMs) {
			rf.candidateRetryElection()
		}
	} else if rf.isLeader() {
		// TODO
	} else {
		panic(fmt.Errorf("illegal raft role: %v", rf.role))
	}
}
