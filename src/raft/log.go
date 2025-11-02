package raft

import "fmt"

type LogEntries struct {
	log       []*LogEntry
	snapshot  interface{}
	snapIndex int
	snapTerm  int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func NewEntries() *LogEntries {
	return &LogEntries{
		log:       nil,
		snapshot:  nil,
		snapIndex: nanIndex,
		snapTerm:  nanTerm,
	}
}

func (l *LogEntries) Append(entry *LogEntry) int {
	l.log = append(l.log, entry)
	return l.LastIndex()
}

func (l *LogEntries) LastIndex() int {
	if l.snapshot != nil {
		return l.snapIndex + len(l.log)
	} else {
		return len(l.log)
	}
}

func (l *LogEntries) LastTerm() int {
	if len(l.log) > 0 {
		return l.log[len(l.log)-1].Term
	} else if l.snapshot != nil {
		return l.snapTerm
	} else {
		return nanTerm
	}
}

func (l *LogEntries) Match(index int, term int) bool {
	if l.isZeroIndex(index) {
		return true
	}

	if l.snapshot != nil && index < l.snapIndex {
		panic(fmt.Errorf("invalid index: %d", index))
	} else if l.snapshot != nil && index == l.snapIndex {
		return term == l.snapTerm
	} else if index <= l.LastIndex() {
		return l.Get(index).Term == term
	} else { // index > l.LastIndex()
		return false
	}
}

func (l *LogEntries) PrevTerm(index int) int {
	index--

	if l.isZeroIndex(index) {
		return nanTerm
	}

	if l.snapshot != nil && index < l.snapIndex {
		panic(fmt.Errorf("invalid index: %d", index))
	} else if l.snapshot != nil && index == l.snapIndex {
		return l.snapTerm
	} else if index <= l.LastIndex() {
		return l.Get(index).Term
	} else { // index > l.LastIndex()
		return nanTerm
	}
}

func (l *LogEntries) Get(index int) *LogEntry {
	if l.snapshot != nil && index <= l.snapIndex {
		panic(fmt.Errorf("invalid index: %d", index))
	}

	return l.log[l.index2i(index)]
}

func (l *LogEntries) Amend(index int, entries []*LogEntry) {
	if l.snapshot != nil && index <= l.snapIndex {
		panic(fmt.Errorf("invalid index: %d", index))
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	//
	// Append any new entries not already in the log
	for i, entry := range entries {
		l.setOrAppend(index+i, entry)
	}
}

func (l *LogEntries) GetEntriesStartingFrom(index int) []*LogEntry {
	if l.snapshot != nil && index <= l.snapIndex {
		panic(fmt.Errorf("invalid index: %d", index))
	}

	if index <= l.LastIndex() {
		return l.log[l.index2i(index):]
	} else {
		return nil
	}
}

func (l *LogEntries) IsUpToDate(lastIndex int, lastTerm int) bool {
	if lastTerm > l.LastTerm() {
		return true
	} else if lastTerm < l.LastTerm() {
		return false
	} else {
		return lastIndex >= l.LastIndex()
	}
}

func (l *LogEntries) ContainsLog(index int) bool {
	if l.snapshot != nil {
		return index > l.snapIndex && index <= l.LastIndex()
	} else {
		return index > zeroIndex && index <= l.LastIndex()
	}
}

func (l *LogEntries) FirstIndexOfTerm(term int, index int) int {
	for l.ContainsLog(index-1) && l.Get(index-1).Term == term {
		index--
	}
	return index
}

func (l *LogEntries) LastIndexOfTerm(term int) int {
	for i := l.LastIndex(); l.ContainsLog(i); i-- {
		if l.Get(i).Term == term {
			return i
		}
	}
	return nanIndex
}

// internal methods

func (l *LogEntries) index2i(index int) int {
	if l.snapshot != nil {
		return index - 1 - l.snapIndex
	} else {
		return index - 1
	}
}

func (l *LogEntries) isZeroIndex(index int) bool {
	return index == zeroIndex
}

func (l *LogEntries) setOrAppend(index int, entry *LogEntry) {
	if index <= l.LastIndex() {
		if l.Get(index).Term != entry.Term {
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
	l.log = l.log[:l.index2i(index)]
}
