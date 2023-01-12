package raft

type Snapshot struct {
	data                 []byte
	lastIncludedLogIndex int
	lastIncludedLogTerm  int
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(logIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.installSnapshot(logIndex, snapshot)
	rf.save()
}
