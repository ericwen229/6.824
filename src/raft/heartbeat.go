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
		entriesToSend, ok := rf.getEntriesToSend(nextIndex)
		if !ok {
			// TODO: install snapshot
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshot.lastIncludedLogIndex,
				LastIncludedTerm:  rf.snapshot.lastIncludedLogTerm,
				data:              rf.snapshot.data,
			}
			go func(i int, peer *labrpc.ClientEnd, args *InstallSnapshotArgs) {
				var reply InstallSnapshotReply
				ok := peer.Call("Raft.InstallSnapshot", args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// All Server Rule 2:
				// if RPC request or response contains term T > currentTerm
				// set currentTerm = T, convert to follower
				if reply.Term > rf.currentTerm {
					rf.log("convert to follower T:%d > T:%d (InstallSnapshot reply higher term from S%d)", rf.currentTerm, reply.Term, i)
					rf.convertToFollower(reply.Term)
					return
				}
			}(i, peer, args)
		}

		prevLogIndex := nextIndex - 1
		prevLogTerm := NilLogTerm
		if rf.isLogIndexInRange(prevLogIndex) {
			prevLogTerm = rf.getEntry(prevLogIndex).Term
		} else if prevLogIndex == rf.getPrevLogIndex() {
			prevLogTerm = rf.getPrevLogTerm()
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entriesToSend,
			LeaderCommit: rf.commitIndex,
		}
		rf.log("send heartbeat to S%d T:%d PLI:%d PLT:%d CI:%d E:%s", i, rf.currentTerm, prevLogIndex, prevLogTerm, rf.commitIndex, rf.formatEntries(entriesToSend))

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
				xTerm := reply.XTerm
				xIndex := reply.XIndex

				if xTerm == NilLogTerm {
					// follower doesn't have entry at prevLogIndex at all
					// decrement to tail of follower
					rf.nextIndex[i] = xIndex + 1
				} else if idx := rf.getLastLogIndexOfTerm(xTerm); idx != ZeroLogIndex {
					// follower term mismatch
					// leader has that term
					// decrement to tail of term
					rf.nextIndex[i] = idx + 1
				} else {
					// follower term mismatch
					// leader doesn't have that term
					// decrement to head of that term
					rf.nextIndex[i] = xIndex
				}

				rf.log("get fail heartbeat from S%d NI:%v", i, rf.nextIndex)
			}
		}(i, peer, args)
	}
}

func (rf *Raft) getEntriesToSend(nextIndex int) ([]*LogEntry, bool) {
	// requested entry has been truncated
	// and replaced by snapshot
	// cannot get entries to send
	//
	if nextIndex <= rf.getPrevLogIndex() {
		return nil, false
	}

	// follower is up-to-date
	// no need to send any entries
	if nextIndex > rf.getLastLogIndex() {
		return nil, true
	}

	entries := rf.getEntriesStartingFrom(nextIndex, appendBatchSize)

	entriesCopy := make([]*LogEntry, len(entries))
	copyN := copy(entriesCopy, entries)
	if copyN != len(entries) {
		panic(errors.New("getEntriesToSend failed to copy all entries"))
	}

	return entriesCopy, true
}
