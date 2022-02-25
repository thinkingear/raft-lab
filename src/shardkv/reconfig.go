package shardkv

import "time"

func (kv *ShardKV) checkReConfig() {
	for {
		time.Sleep(ReConfigCheckTime)

	}
}
