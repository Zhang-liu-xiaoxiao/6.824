package kvraft

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrCantReach     = "ErrCantReach"
	ErrDupReq        = "ErrDuplicatedReq"
	ErrLeaderTimeOut = "ErrLeaderTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Seq      int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	Seq      int32
}

type GetReply struct {
	Err   Err
	Value string
}

type ApplyRes struct {
	Err Err
	// only if get
	Value string
}
