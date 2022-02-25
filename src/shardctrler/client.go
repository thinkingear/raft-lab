package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientID    int64
	sequenceNum int
	serverHint  int
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
	// Your code here.
	ck.sequenceNum = 1
	ck.clientID = nrand()

	return ck
}

func (ck *Clerk) redirectServer() {
	ck.serverHint = (ck.serverHint + 1) % len(ck.servers)
}

func (ck *Clerk) Query(num int) Config {
	args := &ConfigArgs{}
	// Your code here.
	args.Num = num
	args.ClientID = ck.clientID
	args.SequenceNum = ck.sequenceNum
	args.Type = QUERY
	for {
		var reply ConfigReply
		ok := ck.servers[ck.serverHint].Call("ShardCtrler.ProcRequest", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.sequenceNum += 1
			return reply.Config
		}
		ck.redirectServer()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &ConfigArgs{}
	// Your code here.
	args.Servers = servers
	args.SequenceNum = ck.sequenceNum
	args.ClientID = ck.clientID
	args.Type = JOIN

	for {
		var reply ConfigReply
		ok := ck.servers[ck.serverHint].Call("ShardCtrler.ProcRequest", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.sequenceNum += 1
			return
		}
		ck.redirectServer()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &ConfigArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.SequenceNum = ck.sequenceNum
	args.Type = LEAVE

	for {
		var reply ConfigReply
		ok := ck.servers[ck.serverHint].Call("ShardCtrler.ProcRequest", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.sequenceNum += 1
			return
		}
		ck.redirectServer()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &ConfigArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.SequenceNum = ck.sequenceNum
	args.ClientID = ck.clientID
	args.Type = MOVE

	for {
		var reply ConfigReply
		ok := ck.servers[ck.serverHint].Call("ShardCtrler.ProcRequest", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.sequenceNum += 1
			return
		}
		ck.redirectServer()
		time.Sleep(100 * time.Millisecond)
	}
}
