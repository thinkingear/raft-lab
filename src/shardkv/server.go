package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister      *raft.Persister
	stateMachines  [shardctrler.NShards]StateMachine
	shardLocks     [shardctrler.NShards]sync.Mutex
	requestResults map[int64]RequestResult
	lastConfig     shardctrler.Config
}

type StateMachine struct {
	Shard int
	Owned bool
	KVMap map[string]string
}

type RequestResult struct {
	SequenceNum int
	OpReply     OpReply
}

func (kv *ShardKV) Operation(args *OpArgs, reply *OpReply) {
	// Acquire the corresponding shard's lock first if you owned it,
	// else reject this request
	shard := args.Shard
	kv.shardLocks[shard].Lock()
	defer kv.shardLocks[shard].Unlock()

	if !kv.stateMachines[shard].Owned {
		reply.Err = ErrWrongGroup
	}

	clientID, sequenceNum := args.ClientID, args.SequenceNum

	if _, exist := kv.requestResults[clientID]; !exist {
		kv.requestResults[clientID] = RequestResult{}
	}

	if kv.requestResults[clientID].SequenceNum == sequenceNum {
		*reply = kv.requestResults[clientID].OpReply
		return
	}

	_, term, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for kv.requestResults[clientID].SequenceNum != sequenceNum {
		kv.shardLocks[shard].Unlock()
		time.Sleep(ResultCheckTime)
		kv.shardLocks[shard].Lock()
		// has the KVServer's state been stale?
		if currentTerm, _ := kv.rf.GetState(); currentTerm != term {
			reply.Err = ErrWrongLeader
			return
		} else if !kv.stateMachines[shard].Owned {
			reply.Err = ErrWrongGroup
			return
		}
	}

	*reply = kv.requestResults[clientID].OpReply
}

func (kv *ShardKV) takeSnapshot(commandIndex int) {
	if kv.maxraftstate != -1 && kv.maxraftstate <= kv.persister.RaftStateSize() {
		for i := 0; i < len(kv.shardLocks); i++ {
			kv.shardLocks[i].Lock()
			defer kv.shardLocks[i].Unlock()
		}

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.stateMachines)
		e.Encode(kv.requestResults)
		kv.rf.Snapshot(commandIndex, w.Bytes())
	}
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.stateMachines) != nil ||
		d.Decode(&kv.requestResults) != nil {
		log.Fatalf("decode error\n")
	} else {
		//DPrintf("Recover from Snapshot size: %v", kv.persister.SnapshotSize())
		//DPrintf("Recover from Snapshot: %v", stateMachine)
	}
}

func (kv *ShardKV) procOperation() {

}

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.applySnapshot(m.Snapshot)
		} else if m.CommandValid {

			kv.takeSnapshot(m.CommandIndex)
		} else {
			// do nothing
		}
	}
}

func (kv *ShardKV) mkEnd(string2 string) *labrpc.ClientEnd {
	return kv.make_end(string2)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OpArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister

	for shard := 0; shard < shardctrler.NShards; shard++ {
		kv.stateMachines[shard].KVMap = make(map[string]string)
		kv.stateMachines[shard].Shard = shard
	}

	kv.requestResults = make(map[int64]RequestResult)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.checkReConfig()
	go kv.applier()

	return kv
}
