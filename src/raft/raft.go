package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object to append logs
//
type AppendEntries struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// all servers persistent state
	currentTerm int
	voteFor     map[int]int //kv:term,vote for
	logs        map[int]interface{}

	// all servers volatile state
	commitIndex int
	lastApplied int
	status      ServerStatus

	// leader's volatile state
	nextIndex  []int
	matchIndex []int

	// followers heartbeat count
	accumulatedHb int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	rf.currentTerm++
	candidateId := rf.me
	term := rf.currentTerm
	rf.voteFor[term] = candidateId
	peers := rf.peers
	serversNum := len(rf.peers)
	rf.status = Candidate
	Debug(dInfo, "Candidate [%d] start a election in term %d", candidateId, term)
	rf.mu.Unlock()
	// server num must be odd, and ceil it
	succeedVoteNums := serversNum/2 + 1
	var collectedVotes int32 = 1
	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{}
		args.CandidateId = candidateId
		args.Term = term
		reply := RequestVoteReply{}
		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				atomic.AddInt32(&collectedVotes, 1)
				Debug(dVote, "[%d] GET vote from server[%d]", args.CandidateId, i)
			}
		}(i)
	}
	rf.mu.Lock()
	// receive leaders heartbeat, revert to follower
	if rf.status != Candidate || collectedVotes < int32(succeedVoteNums) {
		return
	}
	defer rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	me := rf.me
	curTerm := rf.currentTerm
	rf.mu.Unlock()
	Debug(dInfo, "[%d] PROCESS requestvote from [%d]", me, args.CandidateId)
	if args.Term < curTerm {
		reply.VoteGranted = false
		return
	}

	if !rf.checkLatestLog(args.LastLogIndex, args.LastLogTerm) {
		Debug(dLog, "latest log check fail!")
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if _, ok := rf.voteFor[curTerm]; ok {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.voteFor[curTerm] = args.CandidateId

}

func (rf *Raft) checkLatestLog(lastLogIndex int, LastLogTerm int) bool {
	return true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	Debug(dInfo, "[%d] ASK vote request to server[%d]", args.CandidateId, server)
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	Debug(dInfo, "[%d] FINISH vote request to server[%d]", args.CandidateId, server)
	if !ok {
		Debug(dError, "RPC REPLY ERROR [%d] ASK vote request to server[%d]", args.CandidateId, server)
		return false
	}
	if !reply.VoteGranted {
		Debug(dVote, "[%d] LOSS vote from server[%d]", args.CandidateId, server)
		return false
	}
	return true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(electionInternal int, heartbeat int) {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Duration(electionInternal) * time.Millisecond)
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		switch status {
		case Follower:
			time.Sleep(time.Duration(electionInternal) * time.Millisecond)
			if !rf.checkHeartbeat() {
				rf.mu.Lock()
				rf.status = Candidate
				rf.mu.Unlock()
				go rf.AttemptElection()
			}
		case Leader:
			time.Sleep(time.Duration(heartbeat) * time.Millisecond)
			rf.broadcast()
		default:
			continue
		}

	}
}

func (rf *Raft) broadcast() {

}

func (rf *Raft) checkHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	hb := rf.accumulatedHb
	if hb == 0 {
		return false
	}
	rf.accumulatedHb = 0
	return true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	randomNumber := rand.Intn(201) + 300

	go rf.ticker(randomNumber, 100)

	return rf
}
