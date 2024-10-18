package raft

import (
	"time"
)

const applyIntervalMs int64 = 10

func (rf *Raft) applyLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(time.Duration(applyIntervalMs) * time.Millisecond)
		rf.tryApply(applyCh)
	}
}

func (rf *Raft) tryApply(applyCh chan ApplyMsg) {
	var entriesToApply []*LogEntry

	rf.mu.Lock()
	baseIndex := rf.lastApplied + 1
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		entriesToApply = append(entriesToApply, rf.logs.get(rf.lastApplied))
	}
	rf.mu.Unlock()

	for i, entry := range entriesToApply {
		applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       entry.Command,
			CommandIndex:  baseIndex + i,
			SnapshotValid: false, // TODO
			Snapshot:      nil,   // TODO
			SnapshotTerm:  0,     // TODO
			SnapshotIndex: 0,     // TODO
		}
	}
}
