package raft

import "fmt"

type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) getPrevLogIndex() int {
	if rf.snapshot == nil {
		return ZeroLogIndex
	} else {
		return rf.snapshot.lastIncludedLogIndex
	}
}

func (rf *Raft) getPrevLogTerm() int {
	if rf.snapshot == nil {
		return NilLogTerm
	} else {
		return rf.snapshot.lastIncludedLogTerm
	}
}

// li2si converts log index to slice index
func (rf *Raft) li2si(logIndex int) int {
	// slice index:    0 1 2 3 4
	// log index:   [3]4 5 6 7 8
	return logIndex - rf.getPrevLogIndex() - 1
}

// si2li converts slice index to log index
func (rf *Raft) si2li(sliceIndex int) int {
	// slice index:    0 1 2 3 4
	// log index:   [3]4 5 6 7 8
	return sliceIndex + rf.getPrevLogIndex() + 1
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
	rf.logEntries = rf.logEntries[:rf.li2si(startIndex)]
}

func (rf *Raft) installSnapshot(logIndex int, snapshot []byte) {
	if !rf.isLogIndexInRange(logIndex) {
		return
	}

	sliceIndex := rf.li2si(logIndex)

	entry := rf.getEntry(logIndex)
	lastIncludedIndex := logIndex
	lastIncludedTerm := entry.Term
	rf.snapshot = &Snapshot{
		data:                 snapshot,
		lastIncludedLogIndex: lastIncludedIndex,
		lastIncludedLogTerm:  lastIncludedTerm,
	}
	rf.logEntries = rf.logEntries[sliceIndex+1:]
}

// ====
// read
// ====

func (rf *Raft) getEntry(logIndex int) *LogEntry {
	return rf.logEntries[rf.li2si(logIndex)]
}

func (rf *Raft) getEntriesStartingFrom(logIndex int, maxNum int) []*LogEntry {
	// ASSUMPTION: logIndex legal

	entries := rf.logEntries[rf.li2si(logIndex):]

	// ASSUMPTION: len(entries) > 0
	if len(entries) == 0 {
		panic(fmt.Errorf("getEntriesStartingFrom get nil, logIndex %d, entries %v", logIndex, formatEntries(rf.logEntries)))
	} else if len(entries) > maxNum {
		entries = entries[:maxNum]
	}
	return entries
}

func (rf *Raft) getLastLogIndex() int {
	return rf.getPrevLogIndex() + len(rf.logEntries)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logEntries) == 0 {
		return rf.getPrevLogTerm()
	} else {
		return rf.logEntries[len(rf.logEntries)-1].Term
	}
}

func (rf *Raft) getLastLogIndexOfTerm(term int) int {
	logIndex := ZeroLogIndex
	for i, e := range rf.logEntries {
		if e.Term == term {
			logIndex = rf.si2li(i)
		} else if e.Term > term {
			break
		}
	}
	return logIndex
}

func (rf *Raft) getEntriesStr() string {
	return formatEntries(rf.logEntries)
}

// =====
// check
// =====

func (rf *Raft) isLogIndexInRange(index int) bool {
	return index > rf.getPrevLogIndex() && index <= rf.getLastLogIndex()
}

func (rf *Raft) hasPrevLogEntry(prevLogIndex, prevLogTerm int, xTerm, xIndex *int) bool {
	// no previous log to compare
	if prevLogIndex == ZeroLogIndex {
		return true
	}

	// snapshot match
	if prevLogIndex == rf.getPrevLogIndex() {
		if prevLogTerm == rf.getPrevLogTerm() {
			return true
		} else {
			// mismatch theoretically impossible
			// only committed entries are applied to snapshot
			panic(fmt.Errorf(
				"mismatch between prev and snapshot, prev %d:%d, snapshot %d:%d",
				prevLogIndex,
				prevLogTerm,
				rf.getPrevLogIndex(),
				rf.getPrevLogTerm()))
		}
	}

	// out of range index
	// also accounts for no prev log entry
	if !rf.isLogIndexInRange(prevLogIndex) {
		*xTerm = NilLogTerm
		*xIndex = rf.getLastLogIndex()
		return false
	}

	// term match
	if rf.getEntry(prevLogIndex).Term == prevLogTerm {
		return true
	}

	// term mismatch
	*xTerm = rf.getEntry(prevLogIndex).Term
	i := prevLogIndex
	for rf.isLogIndexInRange(i-1) && rf.getEntry(i-1).Term == rf.getEntry(prevLogIndex).Term {
		i--
	}
	*xIndex = i
	return false
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
