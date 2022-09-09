package shardctrler

import (
	"6.824/raft"
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
	dupMap    map[int64]int
	configs   []Config        // indexed by config num
	waitChMap map[int]chan Op // raft log index -> waitch

}

type Op struct {
	// Your data here.
	ClientID    int64
	Seq         int
	QueryNum    int // desired config number
	MoveShard   int
	MoveGID     int
	LeaveGIDs   []int
	JoinServers map[int][]string // new GID -> servers mappings
	Term        int
	Operation   string
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		ClientID:    args.ClientID,
		Seq:         args.Seq,
		Term:        term,
		JoinServers: args.Servers,
		Operation:   JOIN,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		ClientID:  args.ClientID,
		Seq:       args.Seq,
		Term:      term,
		LeaveGIDs: args.GIDs,
		Operation: LEAVE,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		ClientID:  args.ClientID,
		Seq:       args.Seq,
		Term:      term,
		MoveGID:   args.GID,
		MoveShard: args.Shard,
		Operation: MOVE,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		ClientID:  args.ClientID,
		Seq:       args.Seq,
		Term:      term,
		QueryNum:  args.Num,
		Operation: QUERY,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	select {
	case res := <-ch:
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			sc.mu.Lock()
			if args.Num < 0 || args.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			sc.mu.Unlock()
		}
	case <-time.After(100 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
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
	sc.dupMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	go sc.readRaftCh()
	return sc
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) readRaftCh() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				op := msg.Command.(Op)
				if !sc.checkIfDuplicated(op.ClientID, op.Seq) {
					sc.dupMap[op.ClientID] = op.Seq
					switch op.Operation {
					case JOIN:
						Debug(dSharedController, "[%d] get Join op,:%+v", sc.me, op)
						sc.doJoin(op)
					case MOVE:
						Debug(dSharedController, "[%d] get Move op,:%+v", sc.me, op)
						sc.doMove(op)
					case LEAVE:
						Debug(dSharedController, "[%d] get Leave op,:%+v", sc.me, op)
						sc.doLeave(op)
					case QUERY:
					default:
						Debug(dSharedController, "[%d] get Empty op,:%+v", sc.me, op)

					}
				}
				sc.mu.Unlock()
				curTerm, isLeader := sc.rf.GetState()
				//sc.getWaitCh(m.CommandIndex) <- op
				if isLeader && curTerm == op.Term {
					sc.getWaitCh(msg.CommandIndex) <- op
					//Debug(dServer, "[%d] Response to channel req :%d from [%d]", sc.me, op.Seq, op.ClientID)
				}
			}
		}
	}
}

func (sc *ShardCtrler) checkIfDuplicated(clientID int64, seq int) bool {
	if _, exist := sc.dupMap[clientID]; exist {
		if seq <= sc.dupMap[clientID] {
			return true
		}
	}
	return false

}

func (sc *ShardCtrler) doJoin(op Op) {
	joinServers := op.JoinServers
	config := Config{}
	latestConf := sc.configs[len(sc.configs)-1]
	newGroup := make(map[int][]string)
	var newShards [10]int
	for key, v := range latestConf.Groups {
		newGroup[key] = v
	}
	for gid, svrs := range joinServers {
		newGroup[gid] = svrs
	}
	for i, ogid := range latestConf.Shards {
		newShards[i] = ogid
	}

	config.Shards = newShards
	config.Groups = newGroup
	config.Num = latestConf.Num + 1

	sc.reBalanceGroup(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) doMove(op Op) {
	moveShard := op.MoveShard
	gid := op.MoveGID
	config := Config{}

	latestConf := sc.configs[len(sc.configs)-1]
	newGroup := make(map[int][]string)
	var newShards [10]int
	for key, v := range latestConf.Groups {
		newGroup[key] = v
	}
	for i, ogid := range latestConf.Shards {
		newShards[i] = ogid
	}
	newShards[moveShard] = gid

	config.Num = latestConf.Num + 1
	config.Shards = newShards
	config.Groups = newGroup
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) doLeave(op Op) {
	leaveGIDs := op.LeaveGIDs
	config := Config{}
	latestConf := sc.configs[len(sc.configs)-1]
	newGroup := make(map[int][]string)
	var newShards [10]int
	Debug(dSharedController, "[%d] last config :%+v", sc.me, latestConf)

	for key, value := range latestConf.Groups {
		mark := false
		for _, gid := range leaveGIDs {
			Debug(dSharedController, "[%d] gid:%d,key:%d", sc.me, gid, key)

			if key == gid {
				mark = true
				break
			}
		}
		if mark == false {
			newGroup[key] = value
		}
	}
	for i, ogid := range latestConf.Shards {
		newShards[i] = ogid
		for _, gid := range leaveGIDs {
			if ogid == gid {
				newShards[i] = -1
			}
		}
	}
	config.Shards = newShards
	config.Num = latestConf.Num + 1
	config.Groups = newGroup
	sc.reBalanceGroup(&config)

	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) reBalanceGroup(config *Config) {
	groupNum := len(config.Groups)
	if groupNum == 0 {
		return
	}
	averageNum := NShards / groupNum
	remain := NShards % groupNum
	gidShardsNum := make(map[int]int)
	for key, _ := range config.Groups {
		gidShardsNum[key] = 0
	}
	for _, gid := range config.Shards {
		if gid <= 0 {
			continue
		}
		gidShardsNum[gid] = gidShardsNum[gid] + 1
	}
	Debug(dSharedController, "[%d] Group shards before rebalance  :%+v", sc.me, gidShardsNum)
	sortedGroup := sortGroupShard(gidShardsNum)

	tRemain := remain
	for _, gid := range sortedGroup {
		target := averageNum
		if tRemain > 0 {
			target++
			tRemain--
		}
		if gidShardsNum[gid] > target {
			for i, shard := range config.Shards {
				if shard == gid {
					gidShardsNum[gid] = gidShardsNum[gid] - 1
					config.Shards[i] = -1
				}
				if gidShardsNum[gid] <= target {
					break
				}
			}
			gidShardsNum[gid] = target
		}
	}

	tRemain = remain
	for _, gid := range sortedGroup {
		target := averageNum
		if tRemain > 0 {
			target++
			tRemain--
		}
		if gidShardsNum[gid] < target {
			for i, shard := range config.Shards {
				if shard <= 0 {
					config.Shards[i] = gid
					gidShardsNum[gid] = gidShardsNum[gid] + 1
				}
				if gidShardsNum[gid] >= target {
					break
				}
			}
			gidShardsNum[gid] = target

		}
	}
	Debug(dSharedController, "[%d] Group shards after rebalance  :%+v", sc.me, config.Shards)

}

func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)

	gidSlice := make([]int, 0, length)

	// map转换成有序的slice
	for gid, _ := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}

	// 让负载压力大的排前面
	// except: 4->3 / 5->2 / 6->1 / 7-> 1 (gids -> shard nums)
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {

			if GroupMap[gidSlice[j]] > GroupMap[gidSlice[j-1]] || (GroupMap[gidSlice[j]] == GroupMap[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}
