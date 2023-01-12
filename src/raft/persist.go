package raft

import (
	"6.824/labgob"
	"bytes"
)

func (rf *Raft) save() {
	// save state
	var stateBytes []byte
	{
		var writeBuffer bytes.Buffer
		encoder := labgob.NewEncoder(&writeBuffer)
		var err error
		err = encoder.Encode(rf.currentTerm)
		err = encoder.Encode(rf.votedFor)
		err = encoder.Encode(rf.logEntries)
		if err != nil {
			panic(err)
		}
		stateBytes = writeBuffer.Bytes()
	}

	// save snapshot
	var snapshotBytes []byte
	if rf.snapshot != nil {
		var writeBuffer bytes.Buffer
		encoder := labgob.NewEncoder(&writeBuffer)
		var err error
		err = encoder.Encode(rf.snapshot.lastIncludedLogIndex)
		err = encoder.Encode(rf.snapshot.lastIncludedLogTerm)
		err = encoder.Encode(rf.snapshot.data)
		if err != nil {
			panic(err)
		}
		snapshotBytes = writeBuffer.Bytes()
	}

	if snapshotBytes != nil {
		rf.persister.SaveStateAndSnapshot(stateBytes, snapshotBytes)
	} else {
		rf.persister.SaveRaftState(stateBytes)
	}
}

func (rf *Raft) load() {
	// load state
	stateData := rf.persister.ReadRaftState()
	if stateData != nil && len(stateData) > 0 {
		readBuffer := bytes.NewBuffer(stateData)
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

	// load snapshot
	snapshotData := rf.persister.ReadSnapshot()
	if snapshotData != nil && len(snapshotData) > 0 {
		readBuffer := bytes.NewBuffer(snapshotData)
		decoder := labgob.NewDecoder(readBuffer)
		var lastIncludedIndex int
		var lastIncludedTerm int
		var data []byte
		var err error
		err = decoder.Decode(&lastIncludedIndex)
		err = decoder.Decode(&lastIncludedTerm)
		err = decoder.Decode(&data)
		if err != nil {
			panic(err)
		}

		rf.snapshot = &Snapshot{
			data:                 data,
			lastIncludedLogIndex: lastIncludedIndex,
			lastIncludedLogTerm:  lastIncludedTerm,
		}
	}
}
