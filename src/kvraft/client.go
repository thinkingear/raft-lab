package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serverHint  int
	clientID    int64
	sequenceNum int // log sequence number
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.sequenceNum = 1
	return ck
}

func (ck *Clerk) redirectServer() {
	ck.serverHint = (ck.serverHint + 1) % len(ck.servers)
}

func (ck *Clerk) sendGet(args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[ck.serverHint].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendPutAppend(args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[ck.serverHint].Call("KVServer.PutAppend", args, reply)
	return ok
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	args := &GetArgs{Key: key, ClientID: ck.clientID, SequenceNum: ck.sequenceNum}
	reply := &GetReply{}

	ck.sendGet(args, reply)

	for reply.E != OK && reply.E != ErrNoKey {
		ck.redirectServer()
		reply = &GetReply{}
		ck.sendGet(args, reply)
	}

	ck.sequenceNum += 1
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientID:    ck.clientID,
		SequenceNum: ck.sequenceNum,
	}

	reply := &PutAppendReply{}
	ck.sendPutAppend(args, reply)

	for reply.E != OK {
		ck.redirectServer()
		reply = &PutAppendReply{}
		ck.sendPutAppend(args, reply)
	}

	ck.sequenceNum += 1
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
