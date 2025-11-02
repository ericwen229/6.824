package raft

type LogEntries struct {
	log []*LogEntry // index start from 1
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func newEntries() *LogEntries {
	return &LogEntries{nil}
}

func (l *LogEntries) append(entry *LogEntry) int {
	l.log = append(l.log, entry)
	return len(l.log)
}

func (l *LogEntries) lastIndex() int {
	return len(l.log)
}

func (l *LogEntries) lastTerm() int {
	if len(l.log) == 0 {
		return nanTerm
	} else {
		return l.get(l.lastIndex()).Term
	}
}

func (l *LogEntries) match(index int, term int) bool {
	return l.isZeroIndex(index) || (l.isLegalIndex(index) && l.get(index).Term == term)
}

func (l *LogEntries) get(index int) *LogEntry {
	return l.log[index-1]
}

func (l *LogEntries) setOrAppend(index int, entry *LogEntry) {
	if l.isLegalIndex(index) {
		existingEntry := l.get(index)
		if existingEntry.Term != entry.Term {
			l.truncateFrom(index)
			l.log = append(l.log, entry)
		}
	} else {
		l.log = append(l.log, entry)
	}
}

func (l *LogEntries) truncateFrom(index int) {
	//  1 2 3 4 5
	//  0 1 2 3 4
	// [x x x x x]
	//      ^
	l.log = l.log[:index-1]
}

func (l *LogEntries) amend(index int, entries []*LogEntry) {
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	//
	// Append any new entries not already in the log
	for i, entry := range entries {
		l.setOrAppend(index+i, entry)
	}
}

func (l *LogEntries) isLegalIndex(index int) bool {
	return index > zeroIndex && index <= l.lastIndex()
}

func (l *LogEntries) isZeroIndex(index int) bool {
	return index == zeroIndex
}

func (l *LogEntries) prevTerm(index int) int {
	if l.isZeroIndex(index - 1) {
		return nanTerm
	} else {
		return l.get(index - 1).Term
	}
}

func (l *LogEntries) getEntriesStartingFrom(index int) []*LogEntry {
	if index > l.lastIndex() {
		return nil
	} else {
		return l.log[index-1:]
	}
}

func (l *LogEntries) isUpToDate(lastIndex int, lastTerm int) bool {
	if lastTerm > l.lastTerm() {
		return true
	} else if lastTerm < l.lastTerm() {
		return false
	} else {
		return lastIndex >= l.lastIndex()
	}
}

func (l *LogEntries) firstIndexOfTerm(term int, index int) int {
	for l.isLegalIndex(index-1) && l.get(index-1).Term == term {
		index--
	}
	return index
}

func (l *LogEntries) lastIndexOfTerm(term int) int {
	for i := l.lastIndex(); l.isLegalIndex(i); i-- {
		if l.get(i).Term == term {
			return i
		}
	}
	return nanIndex
}
