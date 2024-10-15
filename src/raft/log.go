package raft

type logEntries struct {
	log []*logEntry // index start from 1
}

type logEntry struct {
	term    int
	command interface{}
}

func initEntries() *logEntries {
	return &logEntries{nil}
}

func (l *logEntries) append(term int, command interface{}) int {
	entry := &logEntry{term, command}
	l.log = append(l.log, entry)
	return len(l.log)
}
