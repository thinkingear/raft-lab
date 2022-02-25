package shardctrler

import (
	"6.824/raft"
	"log"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs    []Config                // indexed by config num
	requestMap map[int64]RequestResult // map form ClientID to SequenceNumber
}

type RequestResult struct {
	sequenceNum int
	result      ConfigReply
}

type Op struct {
	// Your data here.
	Args        ConfigArgs
	ClientID    int64
	SequenceNum int
}

func (sc *ShardCtrler) ProcRequest(args *ConfigArgs, reply *ConfigReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	clientID, sequenceNum := args.ClientID, args.SequenceNum

	if _, exist := sc.requestMap[clientID]; !exist {
		sc.requestMap[clientID] = RequestResult{}
	}

	if sc.requestMap[clientID].sequenceNum == sequenceNum {
		if args.Type == QUERY {
			reply.Config = sc.requestMap[clientID].result.Config
		}
		return
	}

	op := Op{
		Args:        *args,
		ClientID:    clientID,
		SequenceNum: sequenceNum,
	}

	reply.WrongLeader = true

	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	for sc.requestMap[clientID].sequenceNum != sequenceNum {
		sc.mu.Unlock()
		time.Sleep(CheckTime)
		sc.mu.Lock()
		if currentTerm, _ := sc.rf.GetState(); currentTerm != term {
			return
		}
	}

	reply.WrongLeader = false
	if args.Type == QUERY {
		reply.Config = sc.requestMap[clientID].result.Config
	}

	DPrintf("Request %v + CID %v + SN %v: LastConfig: %v",
		type2str(args.Type), clientID, sequenceNum, sc.configs[len(sc.configs)-1])
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		op := m.Command.(Op)
		sc.mu.Lock()

		if _, exist := sc.requestMap[op.ClientID]; !exist {
			sc.requestMap[op.ClientID] = RequestResult{}
		}

		if sc.requestMap[op.ClientID].sequenceNum == op.SequenceNum {
			sc.mu.Unlock()
			continue
		}

		switch op.Args.Type {
		case JOIN:
			sc.procJoin(&op.Args)
		case LEAVE:
			sc.procLeave(&op.Args)
		case MOVE:
			sc.procMove(&op.Args)
		case QUERY:
			sc.procQuery(&op.Args)
		}

		sc.mu.Unlock()
	}

}

func (sc *ShardCtrler) newConfig() *Config {
	oldConfig := sc.configs[len(sc.configs)-1]

	sc.configs = append(sc.configs, Config{Num: oldConfig.Num + 1})

	newConfig := &sc.configs[len(sc.configs)-1]

	newConfig.Shards = cloneShards(oldConfig.Shards)
	newConfig.Groups = cloneGroups(oldConfig.Groups)

	return newConfig
}

func getGIDs(m map[int][]string, GID int) []int {
	var GIDs []int
	for gid := range m {
		if gid != GID || GID == -1 {
			GIDs = append(GIDs, gid)
		}
	}
	sort.Ints(GIDs)
	return GIDs
}

func getG2S(newConfig *Config, GID int) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range newConfig.Groups {
		if gid != GID {
			g2s[gid] = []int{}
		}
	}

	for shard, gid := range newConfig.Shards {
		if gid != GID {
			g2s[gid] = append(g2s[gid], shard)
		}
	}

	return g2s
}

func cloneShards(oldShards [NShards]int) [NShards]int {
	var newShards [NShards]int
	for shard, gid := range oldShards {
		newShards[shard] = gid
	}
	return newShards
}

func cloneGroups(oldGroups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for k, v := range oldGroups {
		groupNames := make([]string, len(v))
		copy(groupNames, v)
		newGroups[k] = groupNames
	}
	return newGroups
}

func (sc *ShardCtrler) procJoin(args *ConfigArgs) {
	sc.requestMap[args.ClientID] = RequestResult{sequenceNum: args.SequenceNum}
	Servers := args.Servers
	if _, isLeader := sc.rf.GetState(); isLeader {
		DPrintf("Applier + Join + Servers %v", Servers)
	}

	newConfig := sc.newConfig()

	GIDs := getGIDs(Servers, -1)

	for _, GID := range GIDs {
		if _, exist := newConfig.Groups[GID]; exist {
			newConfig.Groups[GID] = Servers[GID]
			continue
		}
		newConfig.Groups[GID] = Servers[GID]

		if len(newConfig.Groups) == 1 {
			for shard := range newConfig.Shards {
				newConfig.Shards[shard] = GID
			}
			continue
		}

		func() {
			minShardsPerGid := NShards / len(newConfig.Groups)
			if minShardsPerGid == 0 {
				return
			}
			g2s := getG2S(newConfig, GID)
			gids := getGIDs(newConfig.Groups, GID)
			restNumShards := minShardsPerGid

			DPrintf("g2s: %v", g2s)
			DPrintf("gids: %v", gids)

			for _, gid := range gids {
				shardIdx := len(g2s[gid])
				for shardIdx > minShardsPerGid {
					newConfig.Shards[g2s[gid][shardIdx-1]] = GID
					restNumShards -= 1
					shardIdx -= 1
					if restNumShards == 0 {
						return
					}
				}
			}
		}()
	}
}

func (sc *ShardCtrler) procLeave(args *ConfigArgs) {
	sc.requestMap[args.ClientID] = RequestResult{sequenceNum: args.SequenceNum}
	GIDs := args.GIDs
	if _, isLeader := sc.rf.GetState(); isLeader {
		DPrintf("Applier + Leave + GIDs: %v", GIDs)
	}

	newConfig := sc.newConfig()

	for _, GID := range GIDs {
		if _, exist := newConfig.Groups[GID]; !exist {
			continue
		}

		delete(newConfig.Groups, GID)
		if len(newConfig.Groups) == 0 {
			for i := 0; i < NShards; i++ {
				newConfig.Shards[i] = 0
			}
			return
		}

		func() {
			var shards []int
			for shard, gid := range newConfig.Shards {
				if gid == GID {
					shards = append(shards, shard)
				}
			}

			if len(shards) == 0 {
				return
			}

			maxShardsPerGid := (NShards + len(newConfig.Groups) - 1) / len(newConfig.Groups)
			g2s := getG2S(newConfig, GID)
			gids := getGIDs(newConfig.Groups, -1)

			shardIdx := 0
			for _, gid := range gids {
				cnt := len(g2s[gid])
				for cnt < maxShardsPerGid {
					newConfig.Shards[shards[shardIdx]] = gid
					shardIdx += 1
					cnt += 1
					if shardIdx == len(shards) {
						return
					}
				}
			}
		}()
	}
}

func (sc *ShardCtrler) procMove(args *ConfigArgs) {
	sc.requestMap[args.ClientID] = RequestResult{sequenceNum: args.SequenceNum}
	Shard, GID := args.Shard, args.GID
	if _, isLeader := sc.rf.GetState(); isLeader {
		DPrintf("Applier + Move: (Shard: %v, GID %v)", Shard, GID)
	}

	newConfig := sc.newConfig()
	newConfig.Shards[Shard] = GID
}

func (sc *ShardCtrler) procQuery(args *ConfigArgs) {
	requestResult := RequestResult{sequenceNum: args.SequenceNum}

	if args.Num < 0 || args.Num >= len(sc.configs) {
		requestResult.result.Config = sc.configs[len(sc.configs)-1]
	} else {
		requestResult.result.Config = sc.configs[args.Num]
	}

	if _, isLeader := sc.rf.GetState(); isLeader {
		DPrintf("Applier + Query: Num %v, Config %v", args.Num, requestResult.result.Config)
	}

	sc.requestMap[args.ClientID] = requestResult
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requestMap = make(map[int64]RequestResult)

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	go sc.applier()

	return sc
}
