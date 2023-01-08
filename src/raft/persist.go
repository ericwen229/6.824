package raft

import (
	"6.824/labgob"
	"bytes"
)

func (rf *Raft) persist() {
	var writeBuffer bytes.Buffer
	encoder := labgob.NewEncoder(&writeBuffer)
	var err error
	err = encoder.Encode(rf.currentTerm)
	err = encoder.Encode(rf.votedFor)
	err = encoder.Encode(rf.logEntries)
	if err != nil {
		panic(err)
	}
	rf.persister.SaveRaftState(writeBuffer.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	// Example:
	readBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(readBuffer)
	var currentTerm int
	var votedFor int
	var logEntries []*LogEntry
	var err error
	err = decoder.Decode(&currentTerm)
	err = decoder.Decode(&votedFor)
	err = decoder.Decode(&logEntries)
	if err != nil {
		panic(err)
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logEntries = logEntries
}
