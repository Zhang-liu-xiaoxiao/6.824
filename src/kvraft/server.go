package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug1 = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug1 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key        string
	Value      string
	Operation  string
	ClientID   int64
	Seq        int32
	Index      int
	SubmitTerm int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// client id -> [seq -> exist]
	Kvs       map[string]string
	DupDetect map[int64]map[int32]bool
	leaderCh  chan Op

	LastSnappedIndex int
	//replyCh map[int]chan ApplyRes
	waitChMap map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	Debug(dServer, "[%d] GET req from [%d], args:%+v", kv.me, args.ClientID, args)
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Key:        args.Key,
		Operation:  GET,
		ClientID:   args.ClientID,
		Seq:        args.Seq,
		SubmitTerm: term,
	}
	kv.submitGetOp(op, args, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = OK
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	Debug(dServer, "[%d] PutAppend req from [%d], args:%+v", kv.me, args.ClientID, args)
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Key:        args.Key,
		Value:      args.Value,
		Operation:  args.Op,
		ClientID:   args.ClientID,
		Seq:        args.Seq,
		SubmitTerm: term,
	}
	// Your code here.
	kv.submitPutAppendOp(op, args, reply)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.DupDetect = make(map[int64]map[int32]bool)
	kv.Kvs = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	if len(snapshot) > 0 {
		var dupDetect map[int64]map[int32]bool
		var kvs map[string]string
		if d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&dupDetect) != nil ||
			d.Decode(&kvs) != nil {
			Debug(dServer, "[%d] initial snapshot decode error", kv.me)
		} else {
			kv.DupDetect = dupDetect
			kv.Kvs = kvs
			kv.LastSnappedIndex = lastIncludedIndex
		}
	}
	//kv.replyCh = make(map[int]chan ApplyRes)
	// You may need initialization code here.
	go kv.ReadRaftChMessage()
	return kv
}

func (kv *KVServer) ReadRaftChMessage() {
	for {
		if kv.killed() {
			return
		}
		select {
		case m := <-kv.applyCh:
			if m.SnapshotValid {
				if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
					kv.mu.Lock()
					kv.ingestSnap(m.Snapshot, m.SnapshotIndex)
					kv.mu.Unlock()
				}
			} else if m.CommandValid {
				Debug(dServer, "[%d] Get Op :%+v,command index:%d", kv.me, m.Command.(Op), m.CommandIndex)
				if m.CommandIndex <= kv.LastSnappedIndex {
					return
				}
				op, _ := m.Command.(Op)
				kv.mu.Lock()
				if !kv.CheckAndFillDupMap(op.ClientID, op.Seq) {
					Debug(dServer, "[%d] Apply req :%d from [%d] command index:%d", kv.me, op.Seq, op.ClientID, m.CommandIndex)
					if op.Operation == PUT || op.Operation == APPEND {
						kv.doPutOrAppend(op)
					}
				}
				kv.mu.Unlock()

				if kv.rf.GetStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
					kv.makeSnapshot(m)
				}
				curTerm, isLeader := kv.rf.GetState()
				if isLeader && curTerm == op.SubmitTerm {
					kv.getWaitCh(m.CommandIndex) <- op
					Debug(dServer, "[%d] Leader send to Raft channel req :%d from [%d]", kv.me, op.Seq, op.ClientID)
				}
			}
		}
	}
}

// return if already exist
func (kv *KVServer) CheckAndFillDupMap(clientID int64, seq int32) bool {
	kv.clearDupDetectMap(clientID, seq)
	if _, ok := kv.DupDetect[clientID]; !ok {
		kv.DupDetect[clientID] = make(map[int32]bool)
		kv.DupDetect[clientID][seq] = true
		return false
	} else {
		if _, ok := kv.DupDetect[clientID][seq]; !ok {
			kv.DupDetect[clientID][seq] = true
			return false
		} else {
			Debug(dServer, "[%d] Receive duplicate req :%d from [%d]", kv.me, seq, clientID)
			return true
		}
	}
}

func (kv *KVServer) doPutOrAppend(op Op) {
	if op.Operation == PUT {
		kv.Kvs[op.Key] = op.Value
	} else if op.Operation == APPEND {
		kv.Kvs[op.Key] = kv.Kvs[op.Key] + op.Value
		Debug(dServer, "[%d] append %s to key:%s, result:%s", kv.me, op.Value, op.Key, kv.Kvs[op.Key])
	}
}
func (kv *KVServer) doGet(op Op) string {
	if _, exist := kv.Kvs[op.Key]; exist {
		return kv.Kvs[op.Key]
	} else {
		return ""
	}
}

func (kv *KVServer) submitPutAppendOp(op Op, args *PutAppendArgs, reply *PutAppendReply) {

	index, term, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-time.After(300 * time.Millisecond):
		Debug(dServer, "[%d] apply time out ,args :%+v", kv.me, args)
		newTerm, isLeader := kv.rf.GetState()
		if isLeader && newTerm == term {
			reply.Err = ErrLeaderTimeOut
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}
}

func (kv *KVServer) submitGetOp(op Op, args *GetArgs, reply *GetReply) {

	index, term, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if op.ClientID != res.ClientID || op.Seq != res.Seq {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.Kvs[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-time.After(300 * time.Millisecond):
		Debug(dServer, "[%d] apply time out ,args :%+v", kv.me, args)
		newTerm, isLeader := kv.rf.GetState()
		if isLeader && newTerm == term {
			reply.Err = ErrLeaderTimeOut
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}

}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *KVServer) clearDupDetectMap(clientID int64, seq int32) {
	//kv.mu.Lock()
	for k := range kv.DupDetect[clientID] {
		if k < seq {
			delete(kv.DupDetect[clientID], k)
		}
	}
	//kv.mu.Unlock()
}

func (kv *KVServer) ingestSnap(snapshot []byte, index int) {
	Debug(dServer, "[%d] install snapshot ,snap index %d", kv.me, index)
	//if index <= kv.LastSnappedIndex {
	//	Debug(dServer, "[%d] Already Snapped {%d}index ,get ingest index{%d}", kv.me, kv.LastSnappedIndex, index)
	//	return
	//}

	if snapshot == nil {
		Debug(dServer, "[%d] nil snapshot", kv.me)
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var dupDetect map[int64]map[int32]bool
	var kvs map[string]string
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&dupDetect) != nil ||
		d.Decode(&kvs) != nil {
		Debug(dServer, "snapshot decode error")
		panic(nil)
	}

	kv.LastSnappedIndex = lastIncludedIndex
	kv.DupDetect = dupDetect
	kv.Kvs = kvs
	Debug(dSnap, "[%d] snapshot command index %d, kvs :%+v", kv.me, index, kvs)
}

func (kv *KVServer) makeSnapshot(m raft.ApplyMsg) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.LastSnappedIndex)
	e.Encode(kv.DupDetect)
	e.Encode(kv.Kvs)
	Debug(dSnap, "[%d] Make Snapshot Raft state size :{%d} Encode bytes:{%d}, dup map size:{%d} "+
		"kvs size{%d} , last command index:%d", kv.me, kv.rf.GetStateSize(), len(w.Bytes()), len(kv.DupDetect), len(kv.Kvs), m.CommandIndex)
	kv.rf.Snapshot(m.CommandIndex, w.Bytes())
	kv.LastSnappedIndex = m.CommandIndex
}
