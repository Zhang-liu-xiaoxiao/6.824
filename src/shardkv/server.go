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
	waitChMap         map[int]chan CommandResponse // raft log index -> waitch
	lastIncludedIndex int
	lastApplied       int
	DupMap            map[int]map[int64]int
	Shards            map[int]*ShardData // shards -> kvs
	Conf              shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//Debug(dShardKv, "[%d]{%d} GET req from [%d], args:%+v", kv.me, kv.gid, args.ClientID, args)
	term, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if !kv.shardCanServe(key2shard(args.Key)) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	op := ClientOpCommand{
		Key:       args.Key,
		Operation: GET,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
	}
	command := ServerGenericCommand{
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
		var res = c.Data.(ClientOpCommand)
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.Shards[key2shard(args.Key)].Kvs[args.Key]
			kv.mu.Unlock()
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Debug(dShardKv, "[%d]{%d} PUT APPEND req from [%d], args:%+v", kv.me, kv.gid, args.ClientID, args)
	kv.mu.Lock()
	if !kv.shardCanServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		Debug(dShardKv, "[%d]{%d} wrong group", kv.me, kv.gid)
		kv.mu.Unlock()
		return
	}
	term, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		Debug(dShardKv, "[%d]{%d} wrong leader", kv.me, kv.gid)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := ClientOpCommand{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
	}
	command := ServerGenericCommand{
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
		var res = c.Data.(ClientOpCommand)
		reply.Err = OK
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.Err = ErrWrongLeader
		}
		if c.Err != OK {
			reply.Err = ErrWrongGroup
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

func (kv *ShardKV) readConfigLoop() {
	kv.mu.Lock()
	if !kv.checkShardsStatus() {
		kv.mu.Unlock()
		return
	}
	Debug(dShardKv, "[%d]{%d} All Shards Serving,"+
		" cur conf:%+v try to pull new config", kv.me, kv.gid, kv.Conf)
	nextConf := kv.mck.Query(kv.Conf.Num + 1)
	curConf := kv.Conf
	kv.mu.Unlock()
	if nextConf.Num > curConf.Num {
		Debug(dShardKv, "[%d]{%d} Read new Config:%+v,\n "+
			"old config:%+v", kv.me, kv.gid, nextConf, curConf)
		term, _ := kv.rf.GetState()
		command := ServerGenericCommand{
			SubmitTerm: term,
			Data:       nextConf,
			Operation:  UPDATECONFIG,
		}
		kv.rf.Start(command)

	} else {
		Debug(dShardKv, "[%d]{%d} Fetch No new Config", kv.me, kv.gid)
	}
	//kv.Conf = c
	//index, _, _ := kv.rf.Start(command)
}

func (kv *ShardKV) pushShardsLoop() {
	kv.mu.Lock()
	pushMap := make(map[int][]ShardData)
	for shardNum, gid := range kv.Conf.Shards {
		if kv.Shards[shardNum].ShardStatus == NEEDTOPUSH {
			s := kv.Shards[shardNum].deepCopy()
			for k, v := range kv.DupMap[shardNum] {
				s.DupMap[k] = v
			}
			pushMap[gid] = append(pushMap[gid], s)
		}
	}
	Debug(dShardKv, "[%d]{%d} Push shard loop Config num:%d, Need to Push :%+v", kv.me, kv.gid, kv.Conf.Num, pushMap)
	conf := kv.Conf
	kv.mu.Unlock()
	for gid, data := range pushMap {
		go func(gid int, data []ShardData) {
			servers := conf.Groups[gid]
			args := PushShardsArgs{}
			args.Shards = data
			args.ConfigNum = conf.Num
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				reply := PushShardsReply{}
				if ok := srv.Call("ShardKV.ReceiveShardsHandler", &args, &reply); ok && reply.Err == OK {
					Debug(dShardKv, "[%d]{%d} Success push shards to {%d}", kv.me, kv.gid, gid)
					// receiver already get shards
					// start gc
					go kv.startGC(conf.Num, data)
				}
			}

		}(gid, data)
	}
}

func (kv *ShardKV) checkEmptyLogLoop() {
	if !kv.rf.CheckIfLogsInCurTerm() {
		term, _ := kv.rf.GetState()
		command := ServerGenericCommand{
			Operation:  EMPTYLOG,
			SubmitTerm: term,
		}
		kv.rf.Start(command)
	}
}

func (kv *ShardKV) getChangedShards(newConf shardctrler.Config) (needToPush map[int][]int, get map[int][]int) {
	var newShards []int
	var oldShards []int
	getShards := make(map[int][]int)
	needToPushShards := make(map[int][]int)
	for i, gid := range newConf.Shards {
		if gid == kv.gid {
			newShards = append(newShards, i)
		}
	}
	for i, gid := range kv.Conf.Shards {
		if gid == kv.gid {
			oldShards = append(oldShards, i)
		}
	}
	// need to move shards
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
				pushShardsToGid := newConf.Shards[oldShards[j]]
				needToPushShards[pushShardsToGid] = append(needToPushShards[pushShardsToGid], oldShards[j])
			}
		}
	} else {
		for j := 0; j < len(newShards); j++ {
			gain := true
			for k := 0; k < len(oldShards); k++ {
				if oldShards[k] == newShards[j] {
					gain = false
					break
				}
			}
			if gain {
				getShardsFromGid := kv.Conf.Shards[newShards[j]]
				getShards[getShardsFromGid] = append(getShards[getShardsFromGid], newShards[j])
			}
		}
	}
	return needToPushShards, getShards
}

func (kv *ShardKV) readRaft() {
	for !kv.killed() {
		select {
		case m := <-kv.applyCh:
			if m.SnapshotValid {
				Debug(dShardKv, "[%d]{%d} Snap valid from raft ,m.snapshotindex:%d,m.command index:%d", kv.me, kv.gid, m.SnapshotIndex, m.CommandIndex)
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
				//Debug(dShardKv, "[%d]{%d} Get ClientOpCommand :%+v index:%d", kv.me, kv.gid, m.Command.(ClientOpCommand), m.CommandIndex)
				kv.mu.Lock()
				if m.CommandIndex <= kv.lastIncludedIndex || m.CommandIndex < kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				if kv.lastApplied < m.CommandIndex {
					kv.lastApplied = m.CommandIndex
				}
				//kv.mu.Unlock()
				serverCommand, _ := m.Command.(ServerGenericCommand)
				response := CommandResponse{}
				switch serverCommand.Operation {
				case CLIENT:
					response = kv.applyClient(&m)
				case UPDATECONFIG:
					response = kv.updateConfig(&m)
				case STARTGC:
					response = kv.gcShards(&m)
				case RECEIVESHARDS:
					response = kv.receiveShards(&m)
				case EMPTYLOG:
					response = kv.applyEmptyLog(&m)
				}
				kv.mu.Unlock()
				if kv.rf.GetStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
					kv.makeSnapshot(m)
				}
				//kv.getWaitCh(m.CommandIndex) <- op
				if curTerm, isLeader := kv.rf.GetState(); isLeader && curTerm == serverCommand.SubmitTerm {
					kv.getWaitCh(m.CommandIndex) <- response
				}
			}
		}
	}
}

func (kv *ShardKV) applyClient(m *raft.ApplyMsg) CommandResponse {
	var serverCommand = m.Command.(ServerGenericCommand)
	op := serverCommand.Data.(ClientOpCommand)
	if !kv.shardCanServe(key2shard(op.Key)) {
		return CommandResponse{Err: ErrWrongGroup, Data: op}
	}
	if !kv.checkIfDuplicated(key2shard(op.Key), op.ClientID, op.Seq) {
		kv.DupMap[key2shard(op.Key)][op.ClientID] = op.Seq
		//Debug(dShardKv, "[%d]{%d} Apply req :%d from [%d]", kv.me, kv.gid, op.Seq, op.ClientID)
		if op.Operation == PUT {
			kv.Shards[key2shard(op.Key)].Kvs[op.Key] = op.Value
		} else {
			kv.Shards[key2shard(op.Key)].Kvs[op.Key] += op.Value
		}
	}
	return CommandResponse{Err: OK, Data: op}
}

func (kv *ShardKV) makeSnapshot(m raft.ApplyMsg) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.DupMap)
	e.Encode(kv.Shards)
	e.Encode(kv.Conf)
	kv.mu.Unlock()
	kv.rf.Snapshot(m.CommandIndex, w.Bytes())
}

func (kv *ShardKV) getWaitCh(index int) chan CommandResponse {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan CommandResponse, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) checkIfDuplicated(shardNum int, clientID int64, seq int) bool {
	if _, exist := kv.DupMap[shardNum][clientID]; exist {
		if seq <= kv.DupMap[shardNum][clientID] {
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
	var DupMap map[int]map[int64]int
	var Kvs map[int]*ShardData
	var conf shardctrler.Config
	if d.Decode(&DupMap) != nil ||
		d.Decode(&Kvs) != nil ||
		d.Decode(&conf) != nil {
		Debug(dShardKv, "[%d]{%d} initial snapshot decode error", kv.me, kv.gid)
	} else {
		kv.Shards = Kvs
		kv.DupMap = DupMap
		kv.Conf = conf
		Debug(dShardKv, "[%d]{%d} decode snapshot Shards:%+v", kv.me, kv.gid, Kvs)
	}
}

func (kv *ShardKV) monitor(do func(), timeout time.Duration) {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			do()
		}
		time.Sleep(timeout * time.Millisecond)
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) shardCanServe(shard int) bool {
	if kv.Conf.Shards[shard] == kv.gid && (kv.Shards[shard].ShardStatus == NORMAL) {
		return true
	}
	return false
}

func (kv *ShardKV) gcShards(m *raft.ApplyMsg) CommandResponse {
	var serverCommand = m.Command.(ServerGenericCommand)
	command := serverCommand.Data.(StartGCCommand)
	if command.ConfigNum < kv.Conf.Num {
		return CommandResponse{Err: OK, Data: command}
	} else if command.ConfigNum == kv.Conf.Num {
		for _, shardNum := range command.ShardsNums {
			Debug(dShardKv, "[%d]{%d} Try To GC shard:%d,shard status:%v", kv.me, kv.gid, shardNum, kv.Shards[shardNum].ShardStatus)
			if kv.Shards[shardNum].ShardStatus == NEEDTOPUSH {
				kv.Shards[shardNum].ShardStatus = NORMAL
				kv.Shards[shardNum].Kvs = make(map[string]string)
			}
		}
	} else {
		return CommandResponse{Err: ErrTooNewConfig, Data: command}
	}
	return CommandResponse{Err: OK, Data: command}

}

func (kv *ShardKV) receiveShards(m *raft.ApplyMsg) CommandResponse {
	var serverCommand = m.Command.(ServerGenericCommand)
	command := serverCommand.Data.(ReceiveShardsCommand)
	if command.ConfigNum < kv.Conf.Num {
		return CommandResponse{Err: OK, Data: command}
	} else if command.ConfigNum == kv.Conf.Num {
		for sid, shardData := range command.ShardsMap {
			Debug(dShardKv, "[%d]{%d} Try To RECEIVE shard:%d,shard status:%v", kv.me, kv.gid, sid, kv.Shards[sid].ShardStatus)
			if kv.Shards[sid].ShardStatus == NEEDTORECEIVE {
				shard := shardData.deepCopy()
				kv.Shards[sid] = &shard
				kv.Shards[sid].ShardStatus = NORMAL
				for clientID, seq := range shard.DupMap {
					if _, exist := kv.DupMap[sid][clientID]; !exist || kv.DupMap[sid][clientID] < seq {
						kv.DupMap[sid][clientID] = seq
					}
				}
			}
		}
	} else {
		return CommandResponse{Err: ErrTooNewConfig, Data: command}
	}

	return CommandResponse{Err: OK, Data: command}
}

func (kv *ShardKV) updateConfig(m *raft.ApplyMsg) CommandResponse {
	var serverCommand = m.Command.(ServerGenericCommand)
	newConf := serverCommand.Data.(shardctrler.Config)
	if newConf.Num == kv.Conf.Num+1 {
		Debug(dShardKv, "[%d]{%d} Update Config:%+v,old config:%+v", kv.me, kv.gid, newConf, kv.Conf)
		kv.updateShardStatus(newConf)
		kv.Conf = newConf
		return CommandResponse{Err: OK, Data: newConf}
	}
	Debug(dShardKv, "[%d]{%d} Get Err Update Config Num:%d,my config num:%+v", kv.me, kv.gid, newConf, kv.Conf.Num)

	return CommandResponse{Err: ErrWrongConfigNum, Data: newConf}

}

func (kv *ShardKV) updateShardStatus(newConf shardctrler.Config) {
	needToPushShards, getShards := kv.getChangedShards(newConf)
	if len(needToPushShards) > 0 {
		Debug(dShardKv, "[%d]{%d} Need to Push shards,detail :%+v", kv.me, kv.gid, needToPushShards)
		for _, shards := range needToPushShards {
			for _, shard := range shards {
				kv.Shards[shard].ShardStatus = NEEDTOPUSH
			}
		}
	}
	if len(getShards) > 0 {
		Debug(dShardKv, "[%d]{%d}  Receive shards,detail :%+v", kv.me, kv.gid, getShards)
		for gid, shards := range getShards {
			if gid == 0 {
				// get shards from no one, just serving
				for _, shard := range shards {
					kv.Shards[shard].ShardStatus = NORMAL
				}
			} else {
				for _, shard := range shards {
					kv.Shards[shard].ShardStatus = NEEDTORECEIVE
				}
			}
		}
	}
}

// DeepCopy
func (kv *ShardKV) makeShard(shardNum int, kvs map[string]string, dupMap map[int64]int) *ShardData {
	shard := ShardData{}
	shard.ShardStatus = NORMAL
	shard.DupMap = make(map[int64]int)
	shard.Kvs = make(map[string]string)
	shard.ShardNum = shardNum
	if kvs != nil {
		for k, v := range kvs {
			shard.Kvs[k] = v
		}
	}
	if dupMap != nil {
		for k, v := range dupMap {
			shard.DupMap[k] = v
		}
	}
	return &shard
}

func (kv *ShardKV) checkShardsStatus() bool {
	//myShards := kv.getMyShards()
	printShardsStatus := make(map[int]string)
	res := true
	for i, shard := range kv.Shards {
		printShardsStatus[i] = shard.ShardStatus
		if shard.ShardStatus != NORMAL {
			res = false
		}
	}
	if !res {
		Debug(dShardKv, "[%d]{%d} Update config loop, check status,Config num:%d ,Shards Are Busy, status:%+v", kv.me, kv.gid, kv.Conf.Num, printShardsStatus)
	}
	return res
}

func (kv *ShardKV) getMyShards() []int {
	var myShards []int
	for i, gid := range kv.Conf.Shards {
		if gid == kv.gid {
			myShards = append(myShards, i)
		}
	}
	return myShards
}

func (kv *ShardKV) startGC(configNum int, shards []ShardData) {
	for !kv.killed() {
		term, isLeader := kv.rf.GetState()
		if !isLeader {
			return
		}
		var shardNums []int
		for _, shard := range shards {
			shardNums = append(shardNums, shard.ShardNum)
		}
		gcCommand := StartGCCommand{
			ConfigNum:  configNum,
			ShardsNums: shardNums,
		}
		command := ServerGenericCommand{
			Operation:  STARTGC,
			Data:       gcCommand,
			SubmitTerm: term,
		}
		index, _, _ := kv.rf.Start(command)
		ch := kv.getWaitCh(index)
		select {
		case c := <-ch:
			if c.Err == OK {
				Debug(dShardKv, "[%d]{%d} Success GC Shards:%+v", kv.me, kv.gid, shardNums)
				kv.mu.Lock()
				delete(kv.waitChMap, index)
				kv.mu.Unlock()
				return
			} else {
				continue
			}
		case <-time.After(100 * time.Millisecond):
			continue
		}

	}
}

func (kv *ShardKV) applyEmptyLog(m *raft.ApplyMsg) CommandResponse {
	return CommandResponse{Err: OK}
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
	labgob.Register(ServerGenericCommand{})
	labgob.Register(ClientOpCommand{})
	labgob.Register(StartGCCommand{})
	labgob.Register(ReceiveShardsCommand{})
	labgob.Register(ShardData{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.DupMap = make(map[int]map[int64]int)
	kv.Shards = make(map[int]*ShardData)
	kv.waitChMap = make(map[int]chan CommandResponse)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.DupMap[i] = make(map[int64]int)
		kv.Shards[i] = kv.makeShard(i, nil, nil)
	}
	kv.decodeSnapShot(persister.ReadSnapshot())
	//kv.Conf = kv.mck.Query(-1)
	go kv.readRaft()
	go kv.monitor(kv.readConfigLoop, 100)
	go kv.monitor(kv.pushShardsLoop, 50)
	go kv.monitor(kv.checkEmptyLogLoop, 150)
	return kv
}
