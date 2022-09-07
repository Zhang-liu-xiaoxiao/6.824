package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientID  int64
	Seq       int
	ReConf    bool
}

type ShardData struct {
	Kvs         map[string]string
	ShardStatus string
}

type ServerCommand struct {
	Operation  string
	Data       interface{}
	SubmitTerm int
}

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
	dead int32 // set by Kill()

	mck               *shardctrler.Clerk
	waitChMap         map[int]chan ServerCommand // raft log index -> waitch
	lastIncludedIndex int
	lastApplied       int
	DupMap            map[int64]int
	Kvs               map[int]map[string]string // shards -> kvs
	conf              shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	Debug(dShardKv, "[%d] GET req from [%d], args:%+v", kv.me, args.ClientID, args)
	term, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.conf.Shards[key2shard(args.Key)] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	op := Op{
		Key:       args.Key,
		Operation: GET,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
	}
	command := ServerCommand{
		SubmitTerm: term,
		Data:       op,
		Operation:  CLIENT,
	}
	index, _, _ := kv.rf.Start(command)
	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		kv.mu.Unlock()
	}()
	select {
	case c := <-ch:
		var res = c.Data.(Op)
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.Kvs[key2shard(args.Key)][args.Key]
			kv.mu.Unlock()
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debug(dShardKv, "[%d] PUT APPEND req from [%d], args:%+v", kv.me, args.ClientID, args)
	term, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		Debug(dShardKv, "[%d] wrong leader")
		return
	}
	kv.mu.Lock()
	if kv.conf.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		Debug(dShardKv, "[%d] wrong group")
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
	}
	command := ServerCommand{
		SubmitTerm: term,
		Data:       op,
		Operation:  CLIENT,
	}
	index, _, _ := kv.rf.Start(command)
	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		kv.mu.Unlock()
	}()
	select {
	case c := <-ch:
		var res = c.Data.(Op)
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
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
	atomic.StoreInt32(&kv.dead, 1)

}

func (kv *ShardKV) readConfig() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		c := kv.mck.Query(-1)
		kv.mu.Lock()
		//for c.Num > kv.conf.Num {
		//	newConf := shardctrler.Config{}
		//	for i := kv.conf.Num + 1; i <= c.Num; i++ {
		//		newConf = kv.mck.Query(i)
		//		loseShards, gainShards := kv.GetChangedShards(newConf)
		//		if len(loseShards) > 0 {
		//
		//		}
		//		if len(gainShards) > 0 {
		//
		//		}
		//	}
		//	kv.conf = newConf
		//
		//	c = kv.mck.Query(-1)
		//}
		kv.conf = c
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) GetChangedShards(newConf shardctrler.Config) ([]int, []int) {
	var newShards []int
	var oldShards []int
	for i, gid := range newConf.Shards {
		if gid == kv.gid {
			newShards = append(newShards, i)
		}
	}
	for i, gid := range kv.conf.Shards {
		if gid == kv.gid {
			oldShards = append(oldShards, i)
		}
	}
	// need to move shards
	var loseShards []int
	var gainShards []int
	if len(oldShards) > len(newShards) {
		for j := 0; j < len(oldShards); j++ {
			move := true
			for k := 0; k < len(newShards); k++ {
				if oldShards[j] == newShards[k] {
					move = false
					break
				}
			}
			if move {
				loseShards = append(loseShards, oldShards[j])
			}
		}
	} else {
		for j := 0; j < len(newShards); j++ {
			gain := true
			for k := 0; k < len(oldShards); k++ {
				if oldShards[j] == newShards[k] {
					gain = false
					break
				}
			}
			if gain {
				gainShards = append(gainShards, newShards[j])
			}
		}
	}
	return loseShards, gainShards
}

func (kv *ShardKV) readRaft() {
	for !kv.killed() {
		select {
		case m := <-kv.applyCh:
			if m.SnapshotValid {
				Debug(dShardKv, "[%d] Snap valid from raft ,m.snapshotindex:%d,m.command index:%d", kv.me, m.SnapshotIndex, m.CommandIndex)
				if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
					kv.mu.Lock()
					if m.SnapshotIndex <= kv.lastApplied {
						kv.mu.Unlock()
						continue
					}
					kv.decodeSnapShot(m.Snapshot)
					kv.lastIncludedIndex = m.SnapshotIndex
					kv.mu.Unlock()
				}
			}
			if m.CommandValid {
				//Debug(dShardKv, "[%d] Get Op :%+v index:%d", kv.me, m.Command.(Op), m.CommandIndex)
				kv.mu.Lock()
				if m.CommandIndex <= kv.lastIncludedIndex || m.CommandIndex < kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				if kv.lastApplied < m.CommandIndex {
					kv.lastApplied = m.CommandIndex
				}
				//kv.mu.Unlock()
				serverCommand, _ := m.Command.(ServerCommand)
				switch serverCommand.Operation {
				case CLIENT:
					kv.ApplyClient(m)
				case UPDATECONFIG:
				case DELETESHARDS:
				case TRANSSHARDS:

				}

				kv.mu.Unlock()
				if kv.rf.GetStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
					kv.makeSnapshot(m)
				}
				//kv.getWaitCh(m.CommandIndex) <- op
				if curTerm, isLeader := kv.rf.GetState(); isLeader && curTerm == serverCommand.SubmitTerm {
					kv.getWaitCh(m.CommandIndex) <- serverCommand
				}
			}
		}
	}
}

func (kv *ShardKV) ApplyClient(m raft.ApplyMsg) {
	var serverCommand = m.Command.(ServerCommand)
	op := serverCommand.Data.(Op)
	if kv.gid != kv.conf.Shards[key2shard(op.Key)] {
		op.ReConf = true
		kv.waitChMap[m.CommandIndex] <- serverCommand
		return
	}
	if !kv.checkIfDuplicated(op.ClientID, op.Seq) {
		kv.DupMap[op.ClientID] = op.Seq
		Debug(dShardKv, "[%d] Apply req :%d from [%d]", kv.me, op.Seq, op.ClientID)
		if op.Operation == PUT {
			kv.Kvs[key2shard(op.Key)][op.Key] = op.Value
		} else {
			kv.Kvs[key2shard(op.Key)][op.Key] += op.Value
		}
	}
}

func (kv *ShardKV) makeSnapshot(m raft.ApplyMsg) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.DupMap)
	e.Encode(kv.Kvs)
	kv.mu.Unlock()
	kv.rf.Snapshot(m.CommandIndex, w.Bytes())
}

func (kv *ShardKV) getWaitCh(index int) chan ServerCommand {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan ServerCommand, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) checkIfDuplicated(clientID int64, seq int) bool {
	if _, exist := kv.DupMap[clientID]; exist {
		if seq <= kv.DupMap[clientID] {
			return true
		}
	}
	return false
}

func (kv *ShardKV) decodeSnapShot(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var DupMap map[int64]int
	var Kvs map[int]map[string]string
	if d.Decode(&DupMap) != nil ||
		d.Decode(&Kvs) != nil {
		Debug(dShardKv, "[%d] initial snapshot decode error", kv.me)
	} else {
		kv.Kvs = Kvs
		kv.DupMap = DupMap
		Debug(dShardKv, "[%d] decode snapshot Kvs:%+v", kv.me, Kvs)
	}
}

// move shards to other group
func (kv *ShardKV) reConfiguration(conf shardctrler.Config) {
	// shard need to be moved to which group
	//movedShard2Gid := make(map[int]int)

}

func (kv *ShardKV) monitor(do func(), timeout time.Duration) {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			do()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndShardKvshot() to
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
	labgob.Register(Op{})
	labgob.Register(ServerCommand{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.DupMap = make(map[int64]int)
	kv.Kvs = make(map[int]map[string]string)
	kv.waitChMap = make(map[int]chan ServerCommand)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.decodeSnapShot(persister.ReadSnapshot())
	for i := 0; i < shardctrler.NShards; i++ {
		kv.Kvs[i] = make(map[string]string)
	}
	kv.conf = kv.mck.Query(-1)
	go kv.readRaft()
	go kv.readConfig()
	return kv
}
