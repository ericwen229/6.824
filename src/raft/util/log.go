package util

const zeroIndex = 0
const nanTerm = -1

type LogEntries struct {
	log []*LogEntry // index start from 1
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func NewEntries() *LogEntries {
	return &LogEntries{nil}
}

func (l *LogEntries) Len() int {
	return len(l.log)
}

func (l *LogEntries) Append(entry *LogEntry) int {
	l.log = append(l.log, entry)
	return len(l.log)
}

func (l *LogEntries) LastIndex() int {
	return len(l.log)
}

func (l *LogEntries) Match(index int, term int) bool {
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

func (l *LogEntries) Amend(index int, entries []*LogEntry) {
	for i, entry := range entries {
		l.setOrAppend(index+i, entry)
	}
}

func (l *LogEntries) isLegalIndex(index int) bool {
	return index > zeroIndex && index <= l.LastIndex()
}

func (l *LogEntries) isZeroIndex(index int) bool {
	return index == zeroIndex
}

func (l *LogEntries) PrevTerm(index int) int {
	if l.isZeroIndex(index - 1) {
		return nanTerm
	} else {
		return l.get(index - 1).Term
	}
}

func (l *LogEntries) GetEntriesStartingFrom(index int) []*LogEntry {
	if index > l.LastIndex() {
		return nil
	} else {
		return l.log[index-1:]
	}
}
