package raft

import "time"

func (rf *Raft) commitLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		// >>>>> CRITICAL SECTION >>>>>
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// All Server Rule 1:
		// if commitIndex > lastApplied: increment lastApplied, apply
		// log[lastApplied] to state machine
		rf.lastApplied++
		command := rf.getEntry(rf.lastApplied).Command
		commandIndex := rf.lastApplied
		rf.mu.Unlock()
		// >>>>> CRITICAL SECTION >>>>>

		applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: commandIndex,
		}
	}
}
