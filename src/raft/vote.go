package raft

import (
	"sync/atomic"
)

func (rf *Raft) requestVotesFromPeers() {
	var peerVoteCount int32 = 0
	peerNum := len(rf.peers)

	req := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	for id := range rf.peers {
		if id == rf.me {
			continue
		}

		go func(peerId int) {
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
				voteCount := int(atomic.AddInt32(&peerVoteCount, 1)) + 1
				if 2*voteCount > peerNum && rf.isCandidate() {
					// may have lost election and become follower
					// or have become leader thanks to previous votes
					rf.becomeLeader()
				}
			}
		}(id)
	}
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
