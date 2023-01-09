package raft

import (
	"6.824/labrpc"
	"errors"
	"time"
)

func (rf *Raft) heartbeatLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == roleLeader {
			rf.broadcastHeartbeat()
		}
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i, peer := range rf.peers {
		if rf.me == i {
			continue
		}

		nextIndex := rf.nextIndex[i]
		entriesToSend := rf.getEntriesToSend(nextIndex)
		prevLogIndex := nextIndex - 1
		prevLogTerm := NilLogTerm
		if rf.isLogIndexInRange(prevLogIndex) {
			prevLogTerm = rf.getEntry(prevLogIndex).Term
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: rf.commitIndex,
		}
		rf.log("send heartbeat to S%d T:%d PLI:%d PLT:%d CI:%d E:%s", i, rf.currentTerm, prevLogIndex, prevLogTerm, rf.commitIndex, formatEntries(entriesToSend))

		go func(i int, peer *labrpc.ClientEnd, args *AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := peer.Call("Raft.AppendEntries", args, &reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// All Server Rule 2:
			// if RPC request or response contains term T > currentTerm
			// set currentTerm = T, convert to follower
			if reply.Term > rf.currentTerm {
				rf.log("convert to follower T:%d > T:%d (AppendEntries reply higher term from S%d)", rf.currentTerm, reply.Term, i)
				rf.convertToFollower(reply.Term)
				return
			}

			// abort if reply is stale
			if rf.role != roleLeader || rf.currentTerm != args.Term {
				return
			}

			if reply.Success {
				// Leader Rule 3.1:
				// if successful, update nextIndex and matchIndex for follower
				rf.nextIndex[i] = nextIndex + len(entriesToSend)
				rf.matchIndex[i] = prevLogIndex + len(entriesToSend)
				rf.updateCommitIndex()
				rf.log("get success heartbeat from S%d NI:%v MI:%v CI:%d", i, rf.nextIndex, rf.matchIndex, rf.commitIndex)
			} else {
				// Leader Rule 3.2:
				// if AppendEntries fails because of log inconsistency:
				// decrement nextIndex and retry
				rf.nextIndex[i] = nextIndex - 1
				rf.log("get fail heartbeat from S%d NI:%v", i, rf.nextIndex)
			}
		}(i, peer, args)
	}
}

func (rf *Raft) getEntriesToSend(nextIndex int) []*LogEntry {
	if !rf.isLogIndexInRange(nextIndex) {
		return nil
	}

	entries := rf.getEntriesStartingFrom(nextIndex, appendBatchSize)

	entriesCopy := make([]*LogEntry, len(entries))
	copyN := copy(entriesCopy, entries)
	if copyN != len(entries) {
		panic(errors.New("getEntriesToSend failed to copy all entries"))
	}

	return entriesCopy
}
