package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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
	ReplyCh    chan ApplyRes
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
	kvs       map[string]string
	dupDetect map[int64]map[int32]bool
	leaderCh  chan Op

	replyCh map[int]chan ApplyRes
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	Debug(dServer, "[%d] GET req from [%d], args:%+v", kv.me, args.ClientID, args)
	term, _ := kv.rf.GetState()

	op := Op{
		Key:        args.Key,
		Operation:  GET,
		ClientID:   args.ClientID,
		Seq:        args.Seq,
		ReplyCh:    make(chan ApplyRes, 3),
		SubmitTerm: term,
	}
	kv.submitGetOp(op, args, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = OK

	Debug(dServer, "[%d] PutAppend req from [%d], args:%+v", kv.me, args.ClientID, args)
	term, _ := kv.rf.GetState()
	op := Op{
		Key:        args.Key,
		Value:      args.Value,
		Operation:  args.Op,
		ClientID:   args.ClientID,
		Seq:        args.Seq,
		ReplyCh:    make(chan ApplyRes, 3),
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
	kv.dupDetect = make(map[int64]map[int32]bool)
	kv.kvs = make(map[string]string)
	kv.replyCh = make(map[int]chan ApplyRes)
	// You may need initialization code here.
	go kv.ReadChannel()
	return kv
}

func (kv *KVServer) ReadChannel() {
	for m := range kv.applyCh {
		if m.SnapshotValid {

		} else if m.CommandValid {
			Debug(dServer, "[%d] Get Op :%+v", kv.me, m.Command.(Op))

			op, _ := m.Command.(Op)
			kv.mu.Lock()
			var err Err = OK
			v := kv.doGet(op)
			if kv.CheckAndFillDupMap(op.ClientID, op.Seq) {
				err = ErrDupReq
			} else {
				Debug(dServer, "[%d] Apply req :%d from [%d]", kv.me, op.Seq, op.ClientID)

				if op.Operation == PUT || op.Operation == APPEND {
					kv.doPutOrAppend(op)
				}
			}
			kv.mu.Unlock()
			curTerm, isLeader := kv.rf.GetState()
			if isLeader && curTerm == op.SubmitTerm {
				res := ApplyRes{
					Value: v,
					Err:   err,
				}
				op.ReplyCh <- res
				Debug(dServer, "[%d] After send to channel req :%d from [%d]", kv.me, op.Seq, op.ClientID)

				//kv.mu.Lock()
				//kv.replyCh[m.CommandIndex] <- res
				//kv.mu.Unlock()
			}
		}
	}
}

// return if already exist
func (kv *KVServer) CheckAndFillDupMap(clientID int64, seq int32) bool {
	if _, ok := kv.dupDetect[clientID]; !ok {
		kv.dupDetect[clientID] = make(map[int32]bool)
		kv.dupDetect[clientID][seq] = true
		return false
	} else {
		if _, ok := kv.dupDetect[clientID][seq]; !ok {
			kv.dupDetect[clientID][seq] = true
			return false
		} else {
			Debug(dServer, "[%d] Receive duplicate req :%d from [%d]", kv.me, seq, clientID)
			return true
		}
	}
}

func (kv *KVServer) doPutOrAppend(op Op) {
	if op.Operation == PUT {
		kv.kvs[op.Key] = op.Value
	} else if op.Operation == APPEND {
		if _, exist := kv.kvs[op.Key]; exist {
			kv.kvs[op.Key] = kv.kvs[op.Key] + op.Value
		} else {
			kv.kvs[op.Key] = op.Value
		}
	}
}
func (kv *KVServer) doGet(op Op) string {
	if _, exist := kv.kvs[op.Key]; exist {
		return kv.kvs[op.Key]
	} else {
		return ""
	}
}

func (kv *KVServer) submitPutAppendOp(op Op, args *PutAppendArgs, reply *PutAppendReply) {

	if _, term, ok := kv.rf.Start(op); ok {
		//var ch chan ApplyRes
		//kv.mu.Lock()
		//kv.replyCh[index] = make(chan ApplyRes, 1)
		//ch = kv.replyCh[index]
		//kv.mu.Unlock()
		for {
			select {
			case res := <-op.ReplyCh:
				reply.Err = res.Err
				return
			case <-time.After(3 * time.Second):
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
	} else {
		//Debug(dServer, "[%d] No leader , reqs:%+v return", kv.me, args)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) submitGetOp(op Op, args *GetArgs, reply *GetReply) {

	if _, term, ok := kv.rf.Start(op); ok {
		//var ch chan ApplyRes
		//kv.mu.Lock()
		//kv.replyCh[index] = make(chan ApplyRes, 1)
		//ch = kv.replyCh[index]
		//kv.mu.Unlock()
		for {
			select {
			case res := <-op.ReplyCh:
				reply.Value = res.Value
				reply.Err = res.Err
				return
			case <-time.After(3 * time.Second):
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
	} else {
		Debug(dServer, "[%d] No leader , reqs:%+v return", kv.me, args)
		reply.Err = ErrWrongLeader
		return
	}
}
