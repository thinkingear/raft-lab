package raft

import (
	"6.824/labgob"
	"bytes"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type Snapshot struct {
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "S%v <- S%v SS: iIdx %v, iTerm %v",
		rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	} else if args.Term > rf.CurrentTerm {
		rf.newTermL(args.Term, args.LeaderId)
	}
	reply.Term = rf.CurrentTerm

	if args.LastIncludedIndex < rf.lastApplied {
		// just ignore it
		return
	} else if args.LastIncludedIndex >= rf.Log.lastIndex() ||
		args.LastIncludedTerm != rf.Log.at(args.LastIncludedIndex).Term {

		rf.Log = mkLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.commitIndex = args.LastIncludedIndex
	} else {
		rf.Log.cutStart(args.LastIncludedIndex)
	}

	rf.lastApplied = args.LastIncludedIndex

	Debug(dSnap, "S%v lastApp %v, lStart %v, lLast %v", rf.me, rf.lastApplied, rf.Log.start(), rf.Log.lastIndex())

	rf.snapshot.Snapshot = args.Data
	rf.snapshot.SnapshotIndex = args.LastIncludedIndex
	rf.snapshot.SnapshotTerm = args.LastIncludedTerm
	rf.hasSnap = false

	rf.persistSnapshot()
	rf.signalApplierL()
	rf.resetElectionTimeL(false)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) persistSnapshot() {
	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(rf.CurrentTerm)
	e1.Encode(rf.VotedFor)
	e1.Encode(rf.Log)
	raftState := w1.Bytes()

	w2 := new(bytes.Buffer)
	e2 := labgob.NewEncoder(w2)
	e2.Encode(rf.snapshot)
	stateMachineState := w2.Bytes()

	rf.persister.SaveStateAndSnapshot(raftState, stateMachineState)

	Debug(dPersist, "S%v persist state and snapshot: curTerm %v, vote S%v, len(log)=%v, snapTerm %v, snapIdx %v",
		rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Log.Entries)-1, rf.snapshot.SnapshotTerm, rf.snapshot.SnapshotIndex)
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var snapshot Snapshot
	if d.Decode(&snapshot) != nil {
		// error...
		Debug(dPersist, "S%v read snapshot failed", rf.me)
	} else {
		rf.snapshot = snapshot
		Debug(dPersist, "S%v read snapshot: snapTerm %v, snapIdx",
			rf.me, snapshot.SnapshotTerm, snapshot.SnapshotIndex)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.lastApplied {
		//fmt.Printf("Snapshot failed: index %v < lastApplied %v\n", index, rf.lastApplied)
		return
	}

	rf.Log.cutStart(index)

	Debug(dSnap, "S%v snapshot up to including %v", rf.me, index)
	rf.snapshot.Snapshot = snapshot
	rf.snapshot.SnapshotIndex = index
	rf.snapshot.SnapshotTerm = rf.Log.at(index).Term

	rf.hasSnap = false

	rf.persistSnapshot()
	rf.signalApplierL()
	rf.installSnapshotL()
}

func (rf *Raft) installSnapshotL() {
	if rf.status != Leader {
		return
	}

	Debug(dLeader, "S%v -> ALL SS", rf.me)
	rf.resetElectionTimeL(true)

	for si := range rf.peers {
		if si != rf.me {
			rf.installSnapshot(si)
		}
	}
}

func (rf *Raft) installSnapshot(si int) {
	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshot.SnapshotIndex,
		LastIncludedTerm:  rf.snapshot.SnapshotTerm,
		Data:              make([]byte, len(rf.snapshot.Snapshot)),
	}
	copy(args.Data, rf.snapshot.Snapshot)

	reply := &InstallSnapshotReply{}

	Debug(dLeader, "S%v -> S%v SS: iIdx %v, iTerm %v",
		rf.me, si, args.LastIncludedIndex, args.LastIncludedTerm)
	go func(si int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

		rf.sendInstallSnapshot(si, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.CurrentTerm {
			rf.newTermL(reply.Term, si)
		}
	}(si, args, reply)
}
