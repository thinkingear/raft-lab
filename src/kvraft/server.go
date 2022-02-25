package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

const (
	PUT    = 1
	APPEND = 2
	GET    = 3
	//CheckTime = 1 // best result
	CheckTime = 2
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	T           int
	K           string
	V           string
	ClientID    int64
	SequenceNum int
}

type ApplyResult struct {
	E           Err
	V           string
	SequenceNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	StateMachine map[string]string
	resultMap    map[int64]ApplyResult
	persister    *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.E = ErrWrongLeader

	_, ok := kv.resultMap[args.ClientID]
	if !ok {
		kv.resultMap[args.ClientID] = ApplyResult{}
	}

	if args.SequenceNum != kv.resultMap[args.ClientID].SequenceNum {
		op := Op{
			ClientID:    args.ClientID,
			SequenceNum: args.SequenceNum,
			T:           GET,
			K:           args.Key,
		}

		_, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			return
		}

		for args.SequenceNum != kv.resultMap[args.ClientID].SequenceNum {
			kv.mu.Unlock()
			time.Sleep(CheckTime * time.Millisecond)
			kv.mu.Lock()

			currentTerm, _ := kv.rf.GetState()
			if currentTerm != term {
				return
			}
		}
	}

	reply.E = kv.resultMap[args.ClientID].E
	reply.Value = kv.resultMap[args.ClientID].V
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.E = ErrWrongLeader

	_, ok := kv.resultMap[args.ClientID]
	if !ok {
		kv.resultMap[args.ClientID] = ApplyResult{}
	}

	if args.SequenceNum != kv.resultMap[args.ClientID].SequenceNum {
		op := Op{
			ClientID:    args.ClientID,
			SequenceNum: args.SequenceNum,
			K:           args.Key,
			V:           args.Value,
		}

		if args.Op == "Put" {
			op.T = PUT
		} else {
			op.T = APPEND
		}

		_, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			return
		}

		for args.SequenceNum != kv.resultMap[args.ClientID].SequenceNum {
			kv.mu.Unlock()
			time.Sleep(CheckTime * time.Millisecond)
			kv.mu.Lock()

			currentTerm, _ := kv.rf.GetState()
			if currentTerm != term {
				return
			}
		}
	}

	reply.E = kv.resultMap[args.ClientID].E
}

func (kv *KVServer) applyMsg(m *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := m.Command.(Op)

	if kv.resultMap[op.ClientID].SequenceNum == op.SequenceNum {
		return
	}

	result := ApplyResult{SequenceNum: op.SequenceNum, E: OK}

	switch op.T {
	case PUT:
		{
			kv.StateMachine[op.K] = op.V
			result.V = op.V
		}
	case APPEND:
		{
			v, exist := kv.StateMachine[op.K]
			if exist {
				v += op.V
				kv.StateMachine[op.K] = v
				result.V = v
			} else {
				kv.StateMachine[op.K] = op.V
				result.V = op.V
			}
		}
	case GET:
		{
			v, exist := kv.StateMachine[op.K]
			if exist {
				result.V = v
			} else {
				result.E = ErrNoKey
			}
		}
	}

	kv.resultMap[op.ClientID] = result

	if kv.maxraftstate != -1 && kv.maxraftstate <= kv.persister.RaftStateSize() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.StateMachine)
		e.Encode(kv.resultMap)
		kv.rf.Snapshot(m.CommandIndex, w.Bytes())
	}
}

func op2Str(op int) string {
	res := ""
	switch op {
	case PUT:
		res = "Put"
	case APPEND:
		res = "Append"
	case GET:
		res = "Get"
	}

	return res
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		m := <-kv.applyCh
		if m.SnapshotValid {
			kv.mu.Lock()
			r := bytes.NewBuffer(m.Snapshot)
			d := labgob.NewDecoder(r)
			var stateMachine map[string]string
			var resultMap map[int64]ApplyResult
			if d.Decode(&stateMachine) != nil ||
				d.Decode(&resultMap) != nil {
				log.Fatalf("decode error\n")
			} else {
				//DPrintf("Recover from Snapshot size: %v", kv.persister.SnapshotSize())
				//DPrintf("Recover from Snapshot: %v", stateMachine)

				kv.StateMachine = stateMachine
				kv.resultMap = resultMap
			}
			kv.mu.Unlock()
		} else if m.CommandValid {
			kv.applyMsg(&m)
		} else {
			// do nothing
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultMap = make(map[int64]ApplyResult)
	kv.StateMachine = make(map[string]string)
	kv.persister = persister

	// You may need initialization code here.
	go kv.applier()

	return kv
}
