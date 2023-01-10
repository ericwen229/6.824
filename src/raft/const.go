package raft

import (
	"math"
	"time"
)

// parameters open for tuning
const (
	loopSmallInterval    = 5 * time.Millisecond
	electionTimeoutMinMs = 180
	electionTimeoutMaxMs = 600
	heartbeatInterval    = 100 * time.Millisecond
	appendBatchSize      = math.MaxInt
)

// constants
const (
	ZeroLogIndex = 0
	MinLogIndex  = 1
	NilLogTerm   = -1
)
