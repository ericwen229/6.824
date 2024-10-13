package raft

import (
	"sync"
	"sync/atomic"
)

func (rf *Raft) requestVotesFromPeers() {
	go func(term, candidateId int) {
		var peerVoteCount int32 = 0
		var wg sync.WaitGroup

		req := &RequestVoteArgs{
			Term:        term,
			CandidateId: candidateId,
		}
		for id := range rf.peers {
			if id == candidateId {
				continue
			}

			wg.Add(1)

			go func(term, peerId int) {
				defer wg.Done()

				resp := &RequestVoteReply{}
				if !rf.sendRequestVote(peerId, req, resp) {
					return
				}

				// request vote post process
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if resp.Term > rf.currentTerm {
					rf.foundHigherTerm(resp.Term)
				} else if resp.VoteGranted {
					atomic.AddInt32(&peerVoteCount, 1)
				}
			}(term, id)
		}

		wg.Wait()

		finalVote := int(atomic.LoadInt32(&peerVoteCount)) + 1

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if finalVote*2 > len(rf.peers) &&
			term == rf.currentTerm &&
			rf.isCandidate() { // may have lost election and become follower
			rf.becomeLeader()
		}
	}(rf.currentTerm, rf.me)
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return. Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}
