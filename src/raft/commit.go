package raft

import (
	"fmt"
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
	applyMsgList := rf.genApplyMsgList()

	for _, msg := range applyMsgList {
		if msg.CommandValid {
			rf.logCommit("apply %d", msg.CommandIndex)
		} else {
			rf.logCommit("apply snapshot %d", msg.SnapshotIndex)
		}
		applyCh <- msg
	}
}

func (rf *Raft) genApplyMsgList() []ApplyMsg {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var msgList []ApplyMsg

	if rf.lastApplied > rf.commitIndex {
		panic(fmt.Errorf("invalid apply index: %d", rf.lastApplied))
	} else if rf.lastApplied == rf.commitIndex {
		return msgList
	}

	// now we're sure we have something to apply

	nextIndex := rf.lastApplied + 1
	if rf.logs.IsInSnapshot(nextIndex) {
		// apply snapshot
		snapshot, snapIndex, snapTerm := rf.logs.GetSnapshot()
		rf.lastApplied = snapIndex
		msgList = append(msgList, ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  nanIndex,
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  snapTerm,
			SnapshotIndex: snapIndex,
		})
	} else {
		// apply logs
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.logs.Get(rf.lastApplied)
			msgList = append(msgList, ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  nanTerm,
				SnapshotIndex: nanIndex,
			})
		}
	}

	return msgList
}
