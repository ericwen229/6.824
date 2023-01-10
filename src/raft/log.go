package raft

import "fmt"

type LogEntry struct {
	Command interface{}
	Term    int
}

// ======
// create
// ======

func (rf *Raft) appendCommand(command interface{}) {
	rf.logEntries = append(rf.logEntries, &LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
}

func (rf *Raft) appendLogEntry(entry *LogEntry) {
	rf.logEntries = append(rf.logEntries, entry)
}

// ======
// delete
// ======

func (rf *Raft) removeEntriesStartingFrom(startIndex int) {
	// removeEntriesStartingFrom(4)
	//   log index: [1 2 3] 4 5 => [1 2 3]
	// slice index: [0 1 2] 3 4 => [0 1 2]
	rf.logEntries = rf.logEntries[:startIndex-1]
}

// ====
// read
// ====

func (rf *Raft) getEntry(index int) *LogEntry {
	return rf.logEntries[index-1]
}

func (rf *Raft) getEntriesStartingFrom(index int, maxNum int) []*LogEntry {
	// ASSUMPTION: index legal

	entries := rf.logEntries[index-1:]

	// ASSUMPTION: len(entries) > 0
	if len(entries) == 0 {
		panic(fmt.Errorf("getEntriesStartingFrom get nil, index %d, entries %v", index, formatEntries(rf.logEntries)))
	} else if len(entries) > maxNum {
		entries = entries[:maxNum]
	}
	return entries
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logEntries)
}

func (rf *Raft) getLastLogIndexOfTerm(term int) int {
	idx := ZeroLogIndex
	for i, e := range rf.logEntries {
		if e.Term == term {
			idx = i + 1
		} else if e.Term > term {
			break
		}
	}
	return idx
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logEntries) == 0 {
		return NilLogTerm
	} else {
		return rf.logEntries[len(rf.logEntries)-1].Term
	}
}

func (rf *Raft) getLogLength() int {
	return len(rf.logEntries)
}

func (rf *Raft) getEntriesStr() string {
	return formatEntries(rf.logEntries)
}

// =====
// check
// =====

func (rf *Raft) isLogIndexInRange(index int) bool {
	return index >= MinLogIndex && index <= rf.getLastLogIndex()
}

func (rf *Raft) hasPrevLogEntry(prevLogIndex, prevLogTerm int, xTerm, xIndex *int) bool {
	if prevLogIndex == ZeroLogIndex {
		return true
	}

	// out of range index also accounts for no prev log entry
	if !rf.isLogIndexInRange(prevLogIndex) {
		*xTerm = NilLogTerm
		*xIndex = rf.getLogLength()
		return false
	} else if rf.getEntry(prevLogIndex).Term != prevLogTerm {
		*xTerm = rf.getEntry(prevLogIndex).Term
		i := prevLogIndex
		for rf.isLogIndexInRange(i-1) && rf.getEntry(i-1).Term == rf.getEntry(prevLogIndex).Term {
			i--
		}
		*xIndex = i
		return false
	} else {
		return true
	}
}

func (rf *Raft) isMoreUpToDateThanMe(lastIndex, lastTerm int) bool {
	thisLastIndex := rf.getLastLogIndex()
	thisLastTerm := rf.getLastLogTerm()

	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer
	// is more up-to-date.
	if thisLastTerm != lastTerm {
		return lastTerm > thisLastTerm
	} else {
		return lastIndex >= thisLastIndex
	}
}
