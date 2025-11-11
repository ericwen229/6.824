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

	return msgList
}
