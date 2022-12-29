package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
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
		if rf.role == roleFollower {
			rf.log("advance term T:%d -> T:%d (AppendEntries args from S%d)", rf.currentTerm, args.Term, args.LeaderId)
		} else {
			rf.log("convert to follower T:%d -> T:%d (AppendEntries args from S%d)", rf.currentTerm, args.Term, args.LeaderId)
		}
		rf.convertToFollower(args.Term)
	}

	// AppendEntries Rule 1:
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.log("deny AppendEntries from S%d (T%d < self T:%d)", args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Candidate Rule 3:
	// if AppendEntries RPC received from new leader
	// convert to follower
	if rf.role == roleCandidate {
		rf.log("candidate to follower")
		rf.convertToFollower(rf.currentTerm)
	}

	// Follower Rule 2:
	// if election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate
	rf.log("follower get heartbeat, reset election timer")
	rf.electionTimer = time.Now()

	reply.Term = rf.currentTerm

	// AppendEntries Rule 2:
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if !rf.hasPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		rf.log("deny AppendEntries from S%d (E:%v)", args.LeaderId, formatEntries(rf.logEntries))
		reply.Success = false
		return
	}
	reply.Success = true

	if len(args.Entries) > 0 {
		rf.log("entries before append: %v", formatEntries(rf.logEntries))
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
		rf.log("entries after append: %v", formatEntries(rf.logEntries))
	} else {
		rf.log("no entries to append")
	}

	// AppendEntries Rule 5
	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
		rf.log("advance CI:%d", rf.commitIndex)
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
		if rf.role == roleFollower {
			rf.log("advance term T:%d -> T:%d (RequestVote args from S%d)", rf.currentTerm, args.Term, args.CandidateId)
		} else {
			rf.log("convert to follower T:%d -> T:%d (RequestVote args from S%d)", rf.currentTerm, args.Term, args.CandidateId)
		}
		rf.convertToFollower(args.Term)
	}

	// RequestVote Rule 1:
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.log("deny RequestVote from S%d (T%d < self T:%d)", args.CandidateId, args.Term, rf.currentTerm)
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
		rf.log("vote for S%d, reset election timer", args.CandidateId)
		rf.votedFor = args.CandidateId

		// Follower Rule 2:
		// if election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate
		rf.electionTimer = time.Now()
	} else {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			rf.log("deny vote to S%d (voted for S%d T:%d)", args.CandidateId, rf.votedFor, rf.currentTerm)
		} else {
			rf.log("deny vote to S%d (this I:%d T:%d > I:%d T:%d)", args.CandidateId, rf.getLastLogIndex(), rf.getLastLogTerm(), args.LastLogIndex, args.LastLogTerm)
		}
	}
}
