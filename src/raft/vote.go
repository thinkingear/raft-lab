package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	electionTimeout = 400 * time.Millisecond
)

func (rf *Raft) resetElectionTimeL(heartbeat bool) {

	Debug(dTimer, "S%v term %v %v time reset: cmtIdx %v, lStart %v, lEnd %v",
		rf.me, rf.CurrentTerm, Status2Str(rf.status), rf.commitIndex, rf.Log.start(), rf.Log.lastIndex())

	t := time.Now()
	var ms int64
	if heartbeat {
		ms = 50
	} else {
		t = t.Add(electionTimeout)
		ms = rand.Int63() % electionTimeout.Milliseconds()
	}
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

func (rva *RequestVoteArgs) String() string {
	return fmt.Sprintf("term %v, LLT %v, LLI %v", rva.Term, rva.LastLogTerm, rva.LastLogIndex)
}

func (aea *AppendEntriesArgs) String() string {
	entries := ""
	if len(aea.Entries) == 1 {
		entries = fmt.Sprintf("%v", aea.Entries[0])
	} else if len(aea.Entries) >= 2 {
		entries = fmt.Sprintf("%v...%v", aea.Entries[0], aea.Entries[len(aea.Entries)-1])
	}

	return fmt.Sprintf("term %v, PLT %v, PLI %v, LCommit %v, Entries.len %v: %v",
		aea.Term, aea.PrevLogTerm, aea.PrevLogIndex, aea.LeaderCommit, len(aea.Entries), entries)
}

func (rf *Raft) requestVote(si int, vote *int) {
	reply := &RequestVoteReply{}
	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.Log.at(rf.Log.lastIndex()).Term,
		LastLogIndex: rf.Log.lastIndex(),
	}

	Debug(dVote, "S%v -> S%v RV: %v", rf.me, si, args)

	go func() {
		ok := rf.sendRequestVote(si, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// process RPC response
		if ok {
			if args.Term == rf.CurrentTerm { // still current term ?
				if reply.Term > rf.CurrentTerm {
					rf.newTermL(reply.Term, si)
				} else if reply.VoteGranted && rf.status == Candidate {
					*vote += 1
					Debug(dVote, "S%v <- S%v RV #vote %v", rf.me, si, *vote)
					if *vote == len(rf.peers)/2+1 {
						rf.convert2LeaderL()
						rf.sendAppendsL()
					}
				}
			} else {
				Debug(dTerm, "S%v term %v changed", rf.me, rf.CurrentTerm)
			}
		} else {
			Debug(dDrop, "S%d -> S%d RV dropped: %v", rf.me, si, args)
		}
	}()
}

func (rf *Raft) startElectionL() {
	rf.resetElectionTimeL(false)
	rf.CurrentTerm += 1
	rf.status = Candidate
	rf.VotedFor = rf.me
	rf.persist()
	vote := 1

	Debug(dTerm, "S%v -> All RV term %v", rf.me, rf.CurrentTerm)

	for si := range rf.peers {
		if si != rf.me {
			rf.requestVote(si, &vote)
		}
	}
}

func (rf *Raft) processAppendReplyL(si int, args *AppendEntriesArgs) {
	newMatchIndex := args.PrevLogIndex + len(args.Entries) // update matchIndex
	if newMatchIndex <= rf.matchIndex[si] {
		return
	}
	newNextIndex := newMatchIndex + 1 // nextIndex = matchIndex + 1

	Debug(dLog, "S%v -> S%v match: prevIdx %v, matchIdx %v -> %v, nextIdx %v -> %v", rf.me, si, args.PrevLogIndex, rf.matchIndex[si], newMatchIndex, rf.nextIndex[si], newNextIndex)

	rf.matchIndex[si] = newMatchIndex
	rf.nextIndex[si] = newNextIndex

	// committing
	oldCommitIndex := rf.commitIndex
	for index := rf.Log.start() + 1; index <= rf.Log.lastIndex(); index++ {
		if rf.Log.at(index).Term != rf.CurrentTerm { // only commit in current term
			continue
		}
		// check if it is greater than the majority
		n := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= index {
				n += 1
			}
		}

		if n > len(rf.peers)/2 {
			rf.commitIndex = index
		}
	}

	Debug(dLog, "S%v -> S%v match: commitIdx %v -> %v", rf.me, si, oldCommitIndex, rf.commitIndex)

	rf.signalApplierL()
}

func (rf *Raft) updateNextIndexL(si int, reply *AppendEntriesReply) {
	// update nextIndex
	nextIndex := -1
	if reply.ConflictTerm == -1 {
		Debug(dLog2, "S%v -> S%v conflict: entry miss", rf.me, si)
		nextIndex = reply.ConflictIndex + 1

		if nextIndex > rf.Log.lastIndex()+1 {
			nextIndex = rf.Log.lastIndex() + 1
		} else if nextIndex < rf.Log.start()+1 {
			nextIndex = rf.Log.start() + 1
			rf.installSnapshot(si)
		}
	} else {
		// 1. Upon receiving a conflict response,
		// the leader should first search its Log for `conflictTerm`.
		found := false
		index := reply.ConflictIndex
		for ; index >= rf.Log.start(); index-- {
			if rf.Log.at(index).Term == reply.ConflictTerm {
				found = true
				break
			}
		}

		if found {
			// 2. If it finds an entry in its Log with that term,
			// it should set `nextIndex` to be the one beyond
			// the index of the *last* entry in that term in its Log.
			for ; index >= rf.Log.start() && rf.Log.at(index).Term == reply.ConflictTerm; index-- {
			}
			nextIndex = index + 1
			Debug(dLog2, "S%v -> S%v conflict: conflict term found", rf.me, si)
		} else {
			// 3. If it does not find an entry with that term,
			// it should set `nextIndex = conflictIndex`.
			nextIndex = reply.ConflictIndex
			Debug(dLog2, "S%v -> S%v conflict: conflict term not exist", rf.me, si)
		}
	}

	Debug(dLog, "S%v -> S%v nextIdx: %v -> %v, cause confIdx %v, confTerm %v", rf.me, si, rf.nextIndex[si], nextIndex, reply.ConflictIndex, reply.ConflictTerm)
	rf.nextIndex[si] = nextIndex
}

func (rf *Raft) appendEntries(si int) {
	Debug(dLeader, "S%v -> S%v AE: nextIdx %v", rf.me, si, rf.nextIndex[si])
	if rf.nextIndex[si] < rf.Log.start()+1 {
		rf.installSnapshot(si)
		rf.nextIndex[si] = rf.Log.start() + 1
		return
	}

	reply := &AppendEntriesReply{}
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[si] - 1,
		PrevLogTerm:  rf.Log.at(rf.nextIndex[si] - 1).Term,
		Entries:      make([]Entry, rf.Log.lastIndex()-rf.nextIndex[si]+1),
	}

	copy(args.Entries, rf.Log.slice(rf.nextIndex[si]))

	Debug(dLeader, "S%v -> S%v AE: %v", rf.me, si, args)

	go func() {
		ok := rf.sendAppendEntries(si, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if ok {
			if args.Term == rf.CurrentTerm { // still CurrentTerm ?
				if reply.Term > rf.CurrentTerm {
					rf.newTermL(reply.Term, si)
				} else if reply.Success {
					rf.processAppendReplyL(si, args)
				} else {
					rf.updateNextIndexL(si, reply)
				}
			} else {
				Debug(dTerm, "S%v term %v changed", rf.me, rf.CurrentTerm)
			}
		} else {
			Debug(dDrop, "S%v AE to S%v dropped", rf.me, si)
		}
	}()
}

func (rf *Raft) sendAppendsL() {
	rf.resetElectionTimeL(true)
	Debug(dLeader, "S%v -> All AE term %v", rf.me, rf.CurrentTerm)

	for si := range rf.peers {
		if si != rf.me {
			rf.appendEntries(si)
		}
	}
}

func (rf *Raft) newTermL(term int, si int) {
	Debug(dTerm, "S%v %v term %d -> Follower term %v, cause S%v", rf.me, Status2Str(rf.status), rf.CurrentTerm, term, si)
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.status = Follower
	rf.persist()
}

func (rf *Raft) convert2LeaderL() {
	Debug(dLeader, "S%v win for Leader", rf.me)
	rf.status = Leader

	for si := range rf.peers {
		rf.nextIndex[si] = rf.Log.lastIndex() + 1
		rf.matchIndex[si] = 0
	}

}
