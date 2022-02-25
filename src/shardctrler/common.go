package shardctrler

import (
	"fmt"
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

var Debug bool = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
		fmt.Println()
	}
	return
}

const (
	OK        = "OK"
	CheckTime = 2 * time.Millisecond
	JOIN      = 1
	LEAVE     = 2
	MOVE      = 3
	QUERY     = 4
)

func type2str(opType int) string {
	var res string
	switch opType {
	case JOIN:
		res = "Join"
	case LEAVE:
		res = "Leave"
	case MOVE:
		res = "Move"
	case QUERY:
		res = "Query"
	}
	return res
}

type Err string

type ConfigArgs struct {
	// For all
	ClientID    int64
	SequenceNum int
	Type        int
	// Join
	Servers map[int][]string // new GID -> servers mappings
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int // desired config number
}

type ConfigReply struct {
	// For all
	WrongLeader bool
	Err         Err
	// Query
	Config Config
}
