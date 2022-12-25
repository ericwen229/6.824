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
		term := rf.currentTerm
		role := rf.role
		rf.mu.Unlock()
		// >>>>> CRITICAL SECTION >>>>>

		if role == roleLeader {
			rf.broadcastHeartbeat(term)
		}
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) broadcastHeartbeat(term int) {
	for i, peer := range rf.peers {
		if rf.me == i {
			continue
		}

		go func(peer *labrpc.ClientEnd) {
			var reply AppendEntriesReply
			ok := peer.Call("Raft.AppendEntries", &AppendEntriesArgs{
				Term: term,
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
		}(peer)
	}
}
