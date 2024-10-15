package raft

import (
	"fmt"
)

func (rf *Raft) broadcastHeartbeat() {
	rf.initiateAgreement()
}

func (rf *Raft) initiateAgreement() {
	rf.resetHeartbeatTimeout()

	req := &AppendEntriesArgs{
		Term: rf.currentTerm,
	}
	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		go func(peerId int) {
			resp := &AppendEntriesReply{}
			if !rf.sendAppendEntries(peerId, req, resp) {
				return
			}

			// post process
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			if resp.Term > rf.currentTerm {
				rf.foundHigherTerm(resp.Term)
				return
			}
		}(id)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// default value
	reply.Term = rf.currentTerm

	// if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.foundHigherTerm(args.Term)
		reply.Term = rf.currentTerm
	} else if args.Term < rf.currentTerm {
		// note: need to reply false in the future
		return
	}

	if rf.isFollower() {
		// if election timeout elapses without receiving AppendEntries RPC from current leader...
		rf.resetElectionTimeout()
	} else if rf.isCandidate() {
		// if AppendEntries RPC received from new leader: convert to follower
		rf.candidate2Follower()
	} else if rf.isLeader() {
		panic(fmt.Errorf("leader receive AppendEntries from same term"))
	} else {
		panic(fmt.Errorf("illegal raft role: %v", rf.role))
	}
}
