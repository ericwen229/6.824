package raft

import "time"

func (rf *Raft) commitLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		var entry *LogEntry
		var commandIndex int

		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(loopSmallInterval)
			continue
		}

		// INVARIANT: rf.commitIndex > rf.lastApplied

		// All Server Rule 1:
		// if commitIndex > lastApplied:
		// increment lastApplied, apply log[lastApplied] to state machine
		rf.lastApplied++
		entry = rf.getEntry(rf.lastApplied)
		commandIndex = rf.lastApplied
		rf.log("commit %d:{%d:%v}", commandIndex, entry.Term, entry.Command)
		rf.mu.Unlock()

		applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: commandIndex,
		}
	}
}
