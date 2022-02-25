package shardkv

import (
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	// my definition
	Put               = "Put"
	Append            = "Append"
	Get               = "Get"
	ResultCheckTime   = 2 * time.Millisecond
	ReConfigCheckTime = 100 * time.Millisecond
)

type Err string

type OpArgs struct {
	Key   string
	Value string
	Op    string
	// my definition
	Shard       int
	ClientID    int64
	SequenceNum int
}

type OpReply struct {
	Err Err
	// for 'Get'
	Value string
}
