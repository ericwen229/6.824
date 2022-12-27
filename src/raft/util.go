package raft

import "sync/atomic"

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
