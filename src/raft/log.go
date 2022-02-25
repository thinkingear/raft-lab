package raft

import (
	"fmt"
)

type Log struct {
	Entries []Entry
	Index0  int
}

func mkEmptyLog() Log {
	return Log{Entries: make([]Entry, 1), Index0: 0}
}

func mkLog(index int, term int) Log {
	entry := make([]Entry, 1)
	entry[0] = Entry{Term: term}
	return Log{Entries: entry, Index0: index}
}

func (l *Log) at(index int) *Entry {
	return &l.Entries[index-l.Index0]
}

func (l *Log) lastIndex() int {
	return l.Index0 + len(l.Entries) - 1
}

func (l *Log) append(e Entry) {
	l.Entries = append(l.Entries, e)
}

func (l *Log) appendAll(e []Entry) {
	l.Entries = append(l.Entries, e...)
}

func (l *Log) start() int {
	return l.Index0
}

func (l *Log) slice(index int) []Entry {
	// include index
	return l.Entries[index-l.Index0:]
}

func (l *Log) cutStart(index int) {
	l.Entries = l.Entries[index-l.Index0:]
	l.Index0 = index
}

func (l *Log) cutEnd(index int) {
	l.Entries = l.Entries[:index-l.Index0]
}

func (l *Log) lastEntry() *Entry {
	return l.at(l.lastIndex())
}

func (e *Entry) String() string {
	return fmt.Sprintf("(C %v, T %v)", e.CMD, e.Term)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		if !rf.hasSnap {
			rf.hasSnap = true
			if rf.snapshot.Snapshot == nil || len(rf.snapshot.Snapshot) < 1 {
				continue
			}
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot.Snapshot,
				SnapshotIndex: rf.snapshot.SnapshotIndex,
				SnapshotTerm:  rf.snapshot.SnapshotTerm,
			}
			Debug(dSnap, "S%v apply snapshot: snapIdx %v, snapTerm %v",
				rf.me, msg.SnapshotIndex, msg.SnapshotTerm)
			rf.mu.Unlock()
			rf.applicant.applyCh <- msg
			rf.mu.Lock()
		} else if rf.lastApplied+1 <= rf.commitIndex {
			rf.lastApplied += 1
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.CommandIndex = rf.lastApplied
			msg.Command = rf.Log.at(rf.lastApplied).CMD
			Debug(dCommit, "S%v apply Log[%v] = %v",
				rf.me, msg.CommandIndex, rf.Log.at(msg.CommandIndex))

			//if msg.Command == nil || msg.CommandIndex > 15 {
			//	err := ""
			//	for i := rf.Log.start(); i <= rf.Log.lastIndex(); i++ {
			//		err += fmt.Sprintf("log[%v] = %v, ", i, rf.Log.Entries[i].CMD)
			//	}
			//	log.Printf("S%v lastApp %v, commitIdx %v Log: %v\n",
			//		rf.me, rf.lastApplied, rf.commitIndex, err)
			//}

			rf.mu.Unlock()
			rf.applicant.applyCh <- msg
			rf.mu.Lock()
		} else {
			Debug(dCommit, "S%v applier wait, lastApplied log[%v] = %v", rf.me, rf.lastApplied, rf.Log.at(rf.lastApplied))
			rf.applicant.applierCond.Wait()
		}
	}
}

func (rf *Raft) signalApplierL() {
	Debug(dCommit, "S%v signal applier", rf.me)
	rf.applicant.applierCond.Signal()
}

func (rf *Raft) consistencyCheckL(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	conflictIndex := -1
	conflictTerm := -1
	reply.Term = rf.CurrentTerm

	// If the follower has all the entries the leader sent,
	// the follower MUST NOT truncate its Log.
	// This is because we could be receiving an outdated
	// AppendEntries RPC from the leader

	Debug(dLog, "S%v <- S%v consistency check: prevIdx %v, prevTerm %v, len(Entries) %v, leaderCmt %v, lastIdx %v, lastTerm %v",
		rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, rf.Log.lastIndex(), rf.Log.lastEntry().Term)
	if args.PrevLogIndex > rf.Log.lastIndex() {
		Debug(dLog2, "S%v <- S%v conflict: prevIdx %v > lastIdx %v",
			rf.me, args.LeaderId, args.PrevLogIndex, rf.Log.lastIndex())
		conflictIndex = rf.Log.lastIndex()
	} else if args.PrevLogIndex < rf.Log.start() {
		Debug(dLog2, "S%v <- S%v conflict: prevIdx %v < startIdx %v",
			rf.me, args.LeaderId, args.PrevLogIndex, rf.Log.start())
		conflictIndex = rf.Log.start()
	} else if rf.Log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		conflictTerm = rf.Log.at(args.PrevLogIndex).Term

		for conflictIndex = args.PrevLogIndex; rf.Log.start()+1 <= conflictIndex; {
			conflictIndex -= 1
			if rf.Log.at(conflictIndex).Term != conflictTerm {
				break
			}
		}

		Debug(dLog2, "S%v <- S%v conflict: prevIdx %v, confTerm %v, confIdx %v",
			rf.me, args.LeaderId, args.PrevLogIndex, conflictTerm, conflictIndex)
	} else {
		haveAllEntries := true

		if args.PrevLogIndex+len(args.Entries) > rf.Log.lastIndex() {
			haveAllEntries = false
		} else {
			for i := range args.Entries {
				if args.Entries[i].Term != rf.Log.at(args.PrevLogIndex+i+1).Term {
					haveAllEntries = false
					break
				}
			}
		}

		if !haveAllEntries {
			Debug(dLog, "S%v <- S%v cut end %v", rf.me, args.LeaderId, args.PrevLogIndex+1)
			rf.Log.cutEnd(args.PrevLogIndex + 1)
			rf.Log.appendAll(args.Entries)
			rf.persist()
		} else {
			Debug(dLog, "S%v <- S%v have all entries", rf.me, args.LeaderId)
		}

		reply.Success = true

		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > rf.Log.lastIndex() {
				rf.commitIndex = rf.Log.lastIndex()
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.signalApplierL()
		}

		Debug(dLog, "S%v <- S%v match: cmtIdx %v -> %v",
			rf.me, args.LeaderId, oldCommitIndex, rf.commitIndex)
	}

	//if rf.commitIndex > rf.Log.lastIndex() {
	//	log.Fatalf("S%v commit index %v > last log index %v", rf.me, rf.commitIndex, rf.Log.lastIndex())
	//}

	reply.ConflictTerm = conflictTerm
	reply.ConflictIndex = conflictIndex
}
