package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrCannotServe    = "ErrCannotServe"
	ErrWrongConfigNum = "ErrWrongConfigNum"
	ErrTooNewConfig   = "ErrTooNewConfig"
	ErrRaftTimeout    = "ErrRaftTimeout"
)
const GET = "Get"
const PUT = "Put"
const APPEND = "Append"

const CLIENT = "Client"
const UPDATECONFIG = "UpdateConfig"
const RECEIVESHARDS = "ReceiveShards"
const STARTGC = "StartGC"
const EMPTYLOG = "EmptyLog"

const NORMAL = "Normal"
const NEEDTORECEIVE = "NeedToReceive"
const NEEDTOPUSH = "NeedToPush"
const GCING = "GCing"

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Seq      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}
type ServerGenericCommand struct {
	Operation  string
	Data       interface{}
	SubmitTerm int
}

type ClientOpCommand struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientID  int64
	Seq       int
}

type ReceiveShardsCommand struct {
	ConfigNum int
	ShardsMap map[int]ShardData
}

type StartGCCommand struct {
	ConfigNum  int
	ShardsNums []int
}

type EmptyLogCommand struct {
	Empty interface{}
}

type ShardData struct {
	Kvs         map[string]string
	ShardStatus string
	DupMap      map[int64]int
	ShardNum    int
}

type CommandResponse struct {
	Err  string
	Data interface{}
}

type PushShardsArgs struct {
	Shards    []ShardData
	ConfigNum int
}
type PushShardsReply struct {
	Err string
}

func (sd ShardData) deepCopy() ShardData {
	res := ShardData{}
	res.DupMap = make(map[int64]int)
	res.Kvs = make(map[string]string)
	res.ShardNum = sd.ShardNum
	res.ShardStatus = sd.ShardStatus
	for k, v := range sd.Kvs {
		res.Kvs[k] = v
	}
	for k, v := range sd.DupMap {
		res.DupMap[k] = v
	}
	return res
}
