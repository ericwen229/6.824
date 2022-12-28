package raft

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
)

func genLogId() string {
	return strconv.Itoa(rand.Int())
}

func newContextWithLogId() context.Context {
	return context.WithValue(context.Background(), "logid", genLogId())
}

func (rf *Raft) debug(ctx context.Context, msg string) {
	return
	logId := ctx.Value("logid")
	peer := rf.me
	term := rf.currentTerm

	role := ""
	switch rf.role {
	case roleFollower:
		role = "F"
	case roleCandidate:
		role = "C"
	case roleLeader:
		role = "L"
	}

	fmt.Printf("[logid %s][peer %d][term %d][role %s] %s\n", logId, peer, term, role, msg)
}
