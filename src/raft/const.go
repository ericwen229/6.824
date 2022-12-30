package raft

import "time"

// parameters open for tuning
const (
	loopSmallInterval    = 5 * time.Millisecond
	electionTimeoutMinMs = 200
	electionTimeoutMaxMs = 800
	heartbeatInterval    = 100 * time.Millisecond
	appendBatchSize      = 10
)

// constants
const (
	ZeroLogIndex = 0
	MinLogIndex  = 1
	NilLogTerm   = -1
)
