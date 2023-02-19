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

	rf.log("received snapshot #%d", logIndex)

	rf.installSnapshot(logIndex, snapshot)

	rf.log("log after snapshot: [%d:%d] %v", rf.getPrevLogIndex(), rf.getPrevLogTerm(), rf.getEntriesStr())

	// persistence
	rf.save()
}
