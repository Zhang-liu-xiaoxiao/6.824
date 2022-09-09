package shardkv

import "time"

func (kv *ShardKV) ReceiveShardsHandler(args *PushShardsArgs, reply *PushShardsReply) {
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	Debug(dShardKv, "[%d]{%d} Receive Shards,args.config num:%d,me.config num:%d", kv.me, kv.gid, args.ConfigNum, kv.Conf.Num)
	if args.ConfigNum != kv.Conf.Num {
		if args.ConfigNum > kv.Conf.Num {
			reply.Err = ErrTooNewConfig
		} else if args.ConfigNum < kv.Conf.Num {
			reply.Err = OK
		}
		kv.mu.Unlock()
		return
	}
	//reply.Err = OK
	applyShards := ReceiveShardsCommand{}
	applyShards.ShardsMap = make(map[int]ShardData)
	applyShards.ConfigNum = args.ConfigNum
	for _, shard := range args.Shards {
		applyShards.ShardsMap[shard.ShardNum] = shard
	}
	term, _ = kv.rf.GetState()
	command := ServerGenericCommand{
		Operation:  RECEIVESHARDS,
		SubmitTerm: term,
		Data:       applyShards,
	}
	kv.mu.Unlock()
	index, _, _ := kv.rf.Start(command)
	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		kv.mu.Unlock()
	}()
	select {
	case res := <-ch:
		reply.Err = res.Err
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrRaftTimeout
	}
}
