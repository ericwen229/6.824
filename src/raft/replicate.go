package raft

import (
	"fmt"

	"6.824/raft/util"
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
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*util.LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// default value
	reply.Term = rf.currentTerm
	reply.Success = false

	// if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.foundHigherTerm(args.Term)
		reply.Term = rf.currentTerm
	} else if args.Term < rf.currentTerm {
		// reply false if term < currentTerm
		reply.Success = false
		return
	}

	// valid AppendEntries from leader
	rf.resetElectionTimeout()

	if rf.isFollower() {
		// if election timeout elapses without receiving AppendEntries RPC from current leader...
	} else if rf.isCandidate() {
		// if AppendEntries RPC received from new leader: convert to follower
		rf.candidate2Follower()
	} else if rf.isLeader() {
		panic(fmt.Errorf("leader receive AppendEntries from same term"))
	} else {
		panic(fmt.Errorf("illegal raft role: %v", rf.role))
	}

	// self is guaranteed to be follower from here

	if !rf.logs.Match(args.PrevLogIndex, args.PrevLogTerm) {
		// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		return
	}

	reply.Success = true

	if len(args.Entries) > 0 {
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		//
		// Append any new entries not already in the log
		rf.logs.Amend(args.PrevLogIndex+1, args.Entries)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = util.Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}
}
