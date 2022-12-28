package raft

import (
	"6.824/labrpc"
	"fmt"
	"time"
)

const heartbeatInterval = 100 * time.Millisecond

func (rf *Raft) heartbeatLoop() {
	for rf.killed() == false {

		// >>>>> CRITICAL SECTION >>>>>
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		// >>>>> CRITICAL SECTION >>>>>

		if role == roleLeader {
			rf.broadcastHeartbeat()
		}
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i, peer := range rf.peers {
		if rf.me == i {
			continue
		}

		go func(i int, peer *labrpc.ClientEnd) {
			ctx := newContextWithLogId()

			// >>>>> CRITICAL SECTION >>>>>
			rf.mu.Lock()
			nextIndex := rf.nextIndex[i]
			entriesToSend := rf.getEntriesToSend(nextIndex)
			prevLogIndex := nextIndex - 1
			prevLogTerm := NilTerm
			if rf.isLogIndexInRange(prevLogIndex) {
				prevLogTerm = rf.getEntry(prevLogIndex).Term
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entriesToSend,
				LeaderCommit: rf.commitIndex,
			}
			rf.debug(ctx, fmt.Sprintf("AppendEntries args: %+v", args))
			rf.mu.Unlock()
			// >>>>> CRITICAL SECTION >>>>>

			var reply AppendEntriesReply
			ok := peer.Call("Raft.AppendEntries", args, &reply)

			if !ok {
				return
			}

			// >>>>> CRITICAL SECTION >>>>>
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.debug(ctx, fmt.Sprintf("AppendEntries reply: %+v", &reply))

			// All Server Rule 2:
			// if RPC request or response contains term T > currentTerm
			// set currentTerm = T, convert to follower
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}

			// stale reply check
			if rf.role != roleLeader || rf.currentTerm != args.Term {
				return
			}

			if reply.Success {
				rf.nextIndex[i] = nextIndex + len(entriesToSend)
				rf.matchIndex[i] = prevLogIndex + len(entriesToSend)
				rf.updateCommitStatus()
			} else {
				rf.nextIndex[i] = nextIndex - 1
			}
			// >>>>> CRITICAL SECTION >>>>>
		}(i, peer)
	}
}
