package raft

import (
	"6.824/labrpc"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid  bool
	Command       interface{}
	CommandIndex  int
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = roleFollower
	rf.electionTimer = time.Now() // initial
	rf.electionTimeout = randElectionTimeout()
	rf.votedFor = -1

	rf.log("created")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimeoutCheckLoop()
	go rf.heartbeatLoop()
	go rf.commitLoop(applyCh)

	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != roleLeader {
		return 0, NilLogTerm, false
	}

	// Leader Rule 2:
	// if command received from client:
	// append entry to local log,
	// respond after entry applied to state machine
	// (we implement differently in 6.824 lab)
	rf.logEntries = append(rf.logEntries, &LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.log("receive {%d:%v}, E:%v", rf.currentTerm, command, formatEntries(rf.logEntries))

	return rf.getLastLogIndex(), rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}
