package raft

import (
	"6.824/labrpc"
	"math/rand"
	"sync"
	"time"
)

func randElectionTimeout() time.Duration {
	minMs := 200
	maxMs := 1000
	timeoutMs := minMs + rand.Intn(maxMs-minMs)
	return time.Duration(timeoutMs) * time.Millisecond
}

func (rf *Raft) electionTimeoutCheckLoop() {
	for rf.killed() == false {
		rf.checkElectionTimeout()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) checkElectionTimeout() {
	// >>>>> CRITICAL SECTION >>>>>
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == roleLeader {
		return
	}

	if time.Since(rf.electionTimer) >= rf.electionTimeout {
		rf.initCandidate()
		go rf.runForLeader(rf.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
	}
	// >>>>> CRITICAL SECTION >>>>>
}

func (rf *Raft) runForLeader(term, lastLogIndex, lastLogTerm int) {
	peerNum := len(rf.peers)
	majorityNum := peerNum/2 + 1

	var mu sync.Mutex
	upvoteCount := 1

	for i, peer := range rf.peers {
		if rf.me == i {
			continue
		}

		go func(i int, peer *labrpc.ClientEnd) {
			var reply RequestVoteReply
			ok := peer.Call("Raft.RequestVote", &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}, &reply)

			if !ok {
				return
			}

			if reply.Term > term {
				// >>>>> CRITICAL SECTION >>>>>
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
				}
				return
				// >>>>> CRITICAL SECTION >>>>>
			}

			if reply.VoteGranted {
				mu.Lock()
				defer mu.Unlock()

				upvoteCount++
				if upvoteCount >= majorityNum {
					// >>>>> CRITICAL SECTION >>>>>
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.role == roleCandidate && rf.currentTerm == term {
						rf.convertToLeader()
					}
					return
					// >>>>> CRITICAL SECTION >>>>>
				}
			}
		}(i, peer)
	}
}
