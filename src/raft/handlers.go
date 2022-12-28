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

	// All Server Rule 2:
	// if RPC request or response contains term T > currentTerm
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// AppendEntries Rule 1:
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Candidate Rule 3:
	// if AppendEntries RPC received from new leader
	// convert to follower
	if rf.role == roleCandidate {
		rf.convertToFollower(rf.currentTerm)
	}

	// Follower Rule 2:
	// if election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate
	rf.electionTimer = time.Now()

	reply.Term = rf.currentTerm

	// AppendEntries Rule 2:
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.hasPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		return
	}
	reply.Success = true

	if len(args.Entries) > 0 {
		startIndex := args.PrevLogIndex + 1
		for i, entry := range args.Entries {
			logIndex := startIndex + i
			if rf.isLogIndexInRange(logIndex) {
				if rf.getEntry(logIndex).Term != entry.Term {
					// AppendEntries Rule 3:
					// if an existing entry conflicts with a new one (same index)
					// delete the existing entry and all that follow it
					rf.removeEntriesFrom(logIndex)
					rf.appendLogEntry(entry)
				}
			} else {
				// AppendEntries Rule 4:
				// append any new entries not already in the log
				rf.appendLogEntry(entry)
			}
		}
	}

	// AppendEntries Rule 5
	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
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

	// All Server Rule 2:
	// if RPC request or response contains term T > currentTerm
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// RequestVote Rule 1:
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm

	// RequestVote Rule 2:
	// if votedFor is null or candidateId and candidate's log is at
	// least as up-to-date as receiver's log, grant vote
	reply.VoteGranted =
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			rf.isMoreUpToDateThanMe(args.LastLogIndex, args.LastLogTerm)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId

		// Follower Rule 2:
		// if election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate
		rf.electionTimer = time.Now()
	}
}
