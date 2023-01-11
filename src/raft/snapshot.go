package raft

type Snapshot struct {
	data                 []byte
	lastIncludedLogIndex int
	lastIncludedLogTerm  int
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}
