package raft

import (
	"6.824/labrpc"
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) electionTimeoutCheckLoop() {
	for rf.killed() == false {
		rf.checkElectionTimeout()
		time.Sleep(loopSmallInterval)
	}
}

func (rf *Raft) checkElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == roleLeader {
		return
	}

	// Follower Rule 2:
	// if election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate
	// Candidate Rule 4:
	// if election timeout elapses: start new election
	if time.Since(rf.electionTimer) >= rf.electionTimeout {
		rf.role = roleCandidate

		// each candidate restarts its randomized election timeout
		// at the start of an election
		rf.electionTimeout = randElectionTimeout()

		// Candidate Rule 1:
		// on conversion to candidate, start election:
		// - increment currentTerm
		rf.currentTerm++
		// - vote for self
		rf.votedFor = rf.me
		// - reset election timer
		rf.electionTimer = time.Now()
		// - send RequestVote RPCs to all other servers
		rf.runForLeader()
	}
}

func (rf *Raft) runForLeader() {
	peerNum := len(rf.peers)
	majorityNum := peerNum/2 + 1

	var upvoteCountMutex sync.Mutex
	upvoteCount := 1 // already voted for self

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.log("start election, reset election timer T:%d LLI:%d LLT:%d ET:%v",
		rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm(), rf.electionTimeout)

	for i, peer := range rf.peers {
		if rf.me == i {
			continue
		}

		go func(i int, peer *labrpc.ClientEnd, args *RequestVoteArgs) {
			var reply RequestVoteReply
			ok := peer.Call("Raft.RequestVote", args, &reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// All Server Rule 2:
			// if RPC request or response contains term T > currentTerm
			// set currentTerm = T, convert to follower
			if reply.Term > rf.currentTerm {
				rf.log("convert to follower T:%d > T:%d (RequestVote reply higher term from S%d)", rf.currentTerm, reply.Term, i)
				rf.convertToFollower(reply.Term)
				return
			}

			// abort if reply is stale
			if rf.role != roleCandidate || rf.currentTerm != args.Term {
				return
			}

			if reply.VoteGranted {
				upvoteCountMutex.Lock()
				defer upvoteCountMutex.Unlock()

				upvoteCount++
				rf.log("get upvote from S%d, total %d", i, upvoteCount)

				if upvoteCount >= majorityNum {
					rf.log("become leader T:%d", rf.currentTerm)
					rf.convertToLeader()
				}
			}
		}(i, peer, args)
	}
}

func randElectionTimeout() time.Duration {
	minMs := electionTimeoutMinMs
	maxMs := electionTimeoutMaxMs
	timeoutMs := minMs + rand.Intn(maxMs-minMs)
	return time.Duration(timeoutMs) * time.Millisecond
}
