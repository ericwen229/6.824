package raft

import (
	"math"
	"time"
)

// parameters open for tuning
const (
	loopSmallInterval    = 5 * time.Millisecond
	electionTimeoutMinMs = 200
	electionTimeoutMaxMs = 800
	heartbeatInterval    = 100 * time.Millisecond
	appendBatchSize      = math.MaxInt
)

// constants
const (
	ZeroLogIndex = 0
	NilLogTerm   = -1
)
