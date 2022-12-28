package raft

import (
	"time"
)

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

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// invariant: args.Term == rf.currentTerm

	if rf.role == roleCandidate {
		rf.convertToFollower(args.Term)
	}
	rf.electionTimer = time.Now()
	reply.Term = rf.currentTerm

	if !rf.isNilLogIndex(args.PrevLogIndex) && (!rf.isLogIndexInRange(args.PrevLogIndex) || rf.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	reply.Success = true
	rf.updateLogEntries(args.PrevLogIndex+1, args.Entries)

	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	} else if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// invariant: args.Term == rf.currentTerm

	reply.Term = rf.currentTerm
	reply.VoteGranted = (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isMoreUpToDateThanMe(args.LastLogIndex, args.LastLogTerm)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
	}
}
