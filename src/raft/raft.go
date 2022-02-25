package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

type Entry struct {
	CMD  interface{}
	Term int
}

type Applicant struct {
	applierCond *sync.Cond
	applyCh     chan ApplyMsg
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// My State
	status       int
	electionTime time.Time // when election timeout
	applicant    Applicant
	snapshot     Snapshot
	hasSnap      bool

	// Paper Figure 2:
	// Persistent state on all servers
	// Updated on stable storage before responding to RPCs
	CurrentTerm int // lasted term server has been
	VotedFor    int // candidateId that received vote in current term
	Log         Log // Log entries; each entry contains command for sate machine, and term when entry was received by leader (first index id 1)

	// Volatile state on all servers
	// Reinitialized after each election
	commitIndex int // index of highest Log entry known to be committed (initialized to 0)
	lastApplied int // index of highest Log entry applied to state machine (initialized to 0)

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next Log entry to send to that server (initialized to 1 + leader's last Log index)
	matchIndex []int // for each server, index of highest Log entry known to be replicated on server
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.CurrentTerm
	isleader = rf.status == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil {
		log.Fatalf("S%v persist failed", rf.me)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "raft: S%v persist curTerm %v, votedFor S%v, log.start %v, log.lastIdx %v", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log.start(), rf.Log.lastIndex())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log Log
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil {
		Debug(dPersist, "S%v read persist failed", rf.me)
		//   error...
		log.Fatalf("raft: S%v read persist failed", rf.me)
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
		Debug(dPersist, "S%v read persist curTerm %v, votedFor S%v, log.start %v, log.lastIdx %v", rf.me, CurrentTerm, VotedFor, Log.start(), Log.lastIndex())
		rf.lastApplied = rf.Log.start()
		rf.commitIndex = rf.Log.start()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last Log entry
	LastLogTerm  int // term of candidate's last Log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // curretnTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of Log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // Log entries to store (empty for hearbeat; may send more than one for efficiency)
	LeaderCommit int     // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// fast backup
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dTerm, "S%v <- S%v AE: %v", rf.me, args.LeaderId, args)

	if args.Term > rf.CurrentTerm {
		rf.newTermL(args.Term, args.LeaderCommit)
	}

	if args.Term < rf.CurrentTerm {
		Debug(dTerm, "S%d <- S%v AE rejected: old AE", rf.me, args.LeaderCommit)
	} else {
		rf.consistencyCheckL(args, reply)
		rf.resetElectionTimeL(false)
	}

	reply.Term = rf.CurrentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dTerm, "S%v <- S%v RV: %v", rf.me, args.CandidateId, args)

	myLastEntry := rf.Log.at(rf.Log.lastIndex())
	uptodate := (args.LastLogTerm > myLastEntry.Term) || (args.LastLogTerm == myLastEntry.Term && args.LastLogIndex >= rf.Log.lastIndex())

	if args.Term > rf.CurrentTerm {
		rf.newTermL(args.Term, args.CandidateId)
	}

	if args.Term < rf.CurrentTerm {
		Debug(dTerm, "S%v <- S%v RV rejected: old RV", rf.me, args.CandidateId)
		reply.VoteGranted = false
	} else if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && uptodate {
		Debug(dTerm, "S%v -> S%v vote", rf.me, args.CandidateId)
		rf.VotedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.resetElectionTimeL(false)
	} else {
		Debug(dTerm, "S%v <- S%v RV rejected: update or has voted", rf.me, args.CandidateId)
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
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
// handler function on the server side does not return.  Thus there
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.CurrentTerm
	isLeader := rf.status == Leader

	if isLeader {
		rf.Log.append(Entry{CMD: command, Term: term})
		index = rf.Log.lastIndex()
		Debug(dClient, "S%v new log[%v] = %v",
			rf.me, rf.Log.lastIndex(), rf.Log.at((rf.Log.lastIndex())))
		rf.persist()
		rf.sendAppendsL()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dDrop, "S%v %v killed", rf.me, Status2Str(rf.status))
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dTimer, "S%v term %v %v ticking: cmtIdx %v, lStart %v, lEnd %v",
		rf.me, rf.CurrentTerm, Status2Str(rf.status), rf.commitIndex, rf.Log.start(), rf.Log.lastIndex())

	if rf.status == Leader && time.Now().After(rf.electionTime) {
		rf.sendAppendsL()
	}

	if rf.status != Leader && time.Now().After(rf.electionTime) {
		rf.startElectionL()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(5 * time.Millisecond)
		rf.tick()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	rf.VotedFor = -1
	rf.applicant.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.Log = mkEmptyLog()
	rf.applicant.applierCond = sync.NewCond(&rf.mu)
	rand.Seed(int64(me))

	Debug(dClient, "S%d built", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.lastApplied = rf.Log.start()
	rf.commitIndex = rf.Log.start()

	// start ticker goroutine to start elections
	rf.resetElectionTimeL(false)
	go rf.applier()
	go rf.ticker()

	return rf
}
