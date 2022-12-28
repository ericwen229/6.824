package raft

func (rf *Raft) commitLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.commitCond.Wait()
		}
		rf.lastApplied++
		command := rf.getEntry(rf.lastApplied).Command
		commandIndex := rf.lastApplied
		rf.mu.Unlock()

		applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: commandIndex,
		}
	}
}
