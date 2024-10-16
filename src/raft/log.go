package raft

type logEntries struct {
	log []*logEntry // index start from 1
}

type logEntry struct {
	Term    int
	Command interface{}
}

func initEntries() *logEntries {
	return &logEntries{nil}
}

func (l *logEntries) append(term int, command interface{}) int {
	entry := &logEntry{term, command}
	l.log = append(l.log, entry)
	return len(l.log)
}

func (l *logEntries) lastLogIndex() int {
	return len(l.log)
}

func (l *logEntries) contains(index int, term int) bool {
	return l.isLegalIndex(index) && l.get(index).Term == term
}

func (l *logEntries) get(index int) *logEntry {
	return l.log[index-1]
}

func (l *logEntries) isLegalIndex(index int) bool {
	return index >= 1 && index <= l.lastLogIndex()
}
