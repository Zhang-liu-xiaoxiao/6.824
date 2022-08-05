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
	logs        []LogEntry  // index start from 1
	applyCh     chan ApplyMsg

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
	term = rf.currentTerm
	if rf.status == Leader {
		isleader = true
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term, isLeader = rf.GetState(); isLeader {
		index = len(rf.logs)
		entry := LogEntry{}
		entry.Term = rf.currentTerm
		entry.Command = command
		entry.Index = index
		rf.ProcessNewCommands(entry)
	}

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
func (rf *Raft) ticker(heartbeat int) {
	times := 1
	randomNumber := rand.Intn(201) + 300

	for rf.killed() == false {

		//randomNumber := rand.Intn(201) + 300
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//time.Sleep(time.Duration(randomNumber) * time.Millisecond)
		rf.mu.Lock()
		me := rf.me
		//Debug(dTimer, "[%d] Ticker times:%d", me, times)
		currentTerm := rf.currentTerm
		times++
		switch rf.status {
		case Follower:
			rf.mu.Unlock()
			time.Sleep(time.Duration(randomNumber) * time.Millisecond)
			rf.mu.Lock()
			if rf.accumulatedHb == 0 && rf.status == Follower {
				Debug(dTimer, "Follower [%d] election timeout ", me)
				rf.status = Candidate
				rf.mu.Unlock()
				go rf.AttemptElection()
			} else {
				rf.mu.Unlock()
				Debug(dTimer, "Follower [%d] Remain follower ", me)
				rf.accumulatedHb = 0
			}
		case Leader:
			//reqs := rf.GenerateAppendReq(true, me)
			//var reqs []RequestAppendEntries
			term := rf.currentTerm
			peerNums := len(rf.peers)
			rf.mu.Unlock()
			time.Sleep(time.Duration(heartbeat) * time.Millisecond)
			Debug(dTimer, "Leader[%d] start broadcast in term:%d", me, currentTerm)
			go rf.Broadcast(me, term, peerNums, true)
		default:
			//time.Sleep(time.Duration(50) * time.Millisecond)
			rf.mu.Unlock()
			continue
		}

	}
}

func (rf *Raft) RevertToFollower(term int, isReElect bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = Follower
	if !isReElect {
		rf.accumulatedHb++
	}
	rf.currentTerm = term
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
	rf.status = Follower
	rf.currentTerm = 0
	rf.voteFor = make(map[int]int)
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}
	//go StartHTTPDebuger()

	//rf.
	//Debug(dLog, "Make Raft [%d]", rf.me)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//Debug(dLog, "Reading Persist [%d]", rf.me)

	// start ticker goroutine to start elections

	go rf.ticker(100)
	go rf.checkCommit()
	return rf
}

func (rf *Raft) checkCommit() {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			Debug(dApply, "[%d] commitIndex:%d,lastApplied:%d", rf.commitIndex, rf.lastApplied)
			rf.lastApplied++
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(15) * time.Millisecond)
	}

}

// last rules for learder in figuer 2
// need to be lock
func (rf *Raft) UpdateCommitIndex() {

}
