package raft

import (
	"fmt"

	"6.824/raft/util"
)

func (rf *Raft) broadcastHeartbeat() {
	rf.initiateAgreement()
}

func (rf *Raft) initiateAgreement() {
	rf.logReplicate("starting replicate for term %d", rf.currentTerm)
	rf.logReplicate("nextIndex before: %+v", rf.nextIndex)
	rf.logReplicate("matchIndex before: %+v", rf.matchIndex)

	rf.resetHeartbeatTimeout()

	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		rf.initiateAgreementWithPeer(id)
	}
}

func (rf *Raft) initiateAgreementWithPeer(peerId int) {
	nextIndex := rf.nextIndex[peerId]
	matchIndex := rf.matchIndex[peerId]
	req := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.logs.prevTerm(nextIndex),
		Entries:      rf.logs.getEntriesStartingFrom(nextIndex),
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		resp := &AppendEntriesReply{}
		if !rf.sendAppendEntries(peerId, req, resp) {
			return
		}

		rf.handleAppendEntriesRespFromPeer(req, resp, peerId, nextIndex, matchIndex)
	}()
}

func (rf *Raft) handleAppendEntriesRespFromPeer(
	req *AppendEntriesArgs, resp *AppendEntriesReply, peerId int, oldNextIndex int, oldMatchIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if resp.Term > rf.currentTerm {
		rf.foundHigherTerm(resp.Term)
		return
	}

	// out of date
	if rf.currentTerm > req.Term ||
		!rf.isLeader() ||
		rf.nextIndex[peerId] != oldNextIndex ||
		rf.matchIndex[peerId] != oldMatchIndex {
		return
	}

	if resp.Success {
		// if successful: update nextIndex and matchIndex for follower
		rf.nextIndex[peerId] += len(req.Entries)
		rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
		rf.updateCommitIndex()
	} else {
		// if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		rf.nextIndex[peerId]--
		rf.initiateAgreementWithPeer(peerId)
	}
}

func (rf *Raft) updateCommitIndex() {
	// assertion: is leader

	// if there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	n := rf.logs.lastIndex()
	for n > rf.commitIndex && rf.logs.match(n, rf.currentTerm) {
		if rf.canBeCommited(n) {
			rf.commitIndex = n
			return
		}

		n--
	}
}

func (rf *Raft) canBeCommited(index int) bool {
	// assertion: is leader
	matchPeerNum := 0
	for id := range rf.peers {
		if id == rf.me || rf.matchIndex[id] >= index {
			matchPeerNum++
		}
	}

	return matchPeerNum*2 > len(rf.peers)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
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

	if !rf.logs.match(args.PrevLogIndex, args.PrevLogTerm) {
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
		rf.logs.amend(args.PrevLogIndex+1, args.Entries)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = util.Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}
}
