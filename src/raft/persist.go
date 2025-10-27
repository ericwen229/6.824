package raft

import (
	"6.824/labgob"
	"6.824/raft/util"
	"bytes"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	util.PanicIfErr(encoder.Encode(rf.currentTerm))
	util.PanicIfErr(encoder.Encode(rf.votedFor))
	util.PanicIfErr(encoder.Encode(rf.logs.log))
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	var currentTerm int
	var votedFor int
	var log []*LogEntry
	util.PanicIfErr(decoder.Decode(&currentTerm))
	util.PanicIfErr(decoder.Decode(&votedFor))
	util.PanicIfErr(decoder.Decode(&log))
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs.log = log
}
