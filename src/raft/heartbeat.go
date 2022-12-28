package raft

import (
	"6.824/labrpc"
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

			// term check
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}

			// original state check
			if !rf.isLeader() || rf.currentTerm != args.Term {
				return
			}

			if reply.Success {
				rf.nextIndex[i] = max(rf.nextIndex[i], nextIndex+len(entriesToSend))
				rf.matchIndex[i] = max(rf.matchIndex[i], rf.nextIndex[i]-1)
				rf.updateCommitStatus()
			} else {
				if rf.nextIndex[i] == nextIndex {
					rf.nextIndex[i]--
				}
			}
			// >>>>> CRITICAL SECTION >>>>>
		}(i, peer)
	}
}
