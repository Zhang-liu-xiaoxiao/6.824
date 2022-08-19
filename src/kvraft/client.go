package kvraft

import (
	"6.824/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

const GET = "Get"
const PUT = "Put"
const APPEND = "Append"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	nums       int
	lastLeader int
	// You will have to modify this struct.
	clerkID int64
	seq     int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.nums = len(servers)
	ck.clerkID = nrand()
	ck.lastLeader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	seq := atomic.AddInt32(&ck.seq, 1)
	args := GetArgs{
		Key:      key,
		Seq:      seq,
		ClientID: ck.clerkID,
	}
	index := ck.lastLeader
	reply := GetReply{}
	for {
		reply = GetReply{}
		if ok := ck.servers[index%ck.nums].Call("KVServer.Get", &args, &reply); !ok {
			index++
			continue
		}
		//Debug(dClerk, "[%d] receive GET reply %+v", ck.clerkID, reply)

		if reply.Err == ErrDupReq || reply.Err == OK || reply.Err == "" {
			ck.lastLeader = index
			break
		} else if reply.Err == ErrWrongLeader || reply.Err == ErrCantReach {
			index++
			continue
		} else if reply.Err == ErrLeaderTimeOut {
			continue
		}
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seq := atomic.AddInt32(&ck.seq, 1)
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      seq,
		ClientID: ck.clerkID,
	}
	index := ck.lastLeader
	for {
		reply := PutAppendReply{}
		if ok := ck.servers[index%ck.nums].Call("KVServer.PutAppend", &args, &reply); !ok {
			index++
			continue
		}
		//Debug(dClerk, "[%d] receive PUT APPEND reply %+v", ck.clerkID, reply)
		if reply.Err == ErrDupReq || reply.Err == OK || reply.Err == "" {
			ck.lastLeader = index
			break
		} else if reply.Err == ErrWrongLeader || reply.Err == ErrCantReach {
			index++
			continue
		} else if reply.Err == ErrLeaderTimeOut {
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
