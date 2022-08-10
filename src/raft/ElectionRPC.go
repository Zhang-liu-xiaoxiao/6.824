package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

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
	randomNumber := rand.Intn(401) + 400

	for !rf.killed() {
		rf.mu.Lock()
		if rf.status != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.CurrentTerm++
		candidateId := rf.me
		rf.VoteFor[rf.CurrentTerm] = candidateId
		rf.persist()
		peers := rf.peers
		serversNum := len(rf.peers)
		succeedVoteNums := serversNum/2 + 1
		Debug(dInfo, "Candidate [%d] start a election in term %d", candidateId, rf.CurrentTerm)
		rf.mu.Unlock()
		// server num must be odd, and ceil it

		var collectedVotes uint32 = 1
		for i, _ := range peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			args := RequestVoteArgs{}
			args.CandidateId = candidateId
			args.Term = rf.CurrentTerm
			args.LastLogTerm = 0
			args.LastLogIndex = 0
			if len(rf.Logs) > 0 {
				args.LastLogIndex = len(rf.Logs)
				args.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
			}
			reply := RequestVoteReply{}
			if rf.status != Candidate {
				Debug(dVote, "[%d] No longer candidate Stop Election,term:%d", rf.me, rf.CurrentTerm)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go func(i int) {
				//rf.SendRequestVote(i, &args, &reply)
				if ok := rf.SendRequestVote(i, &args, &reply); !ok {
					return
				}
				rf.mu.Lock()
				termNeedPersist := false
				defer rf.mu.Unlock()
				defer rf.needPersist(&termNeedPersist)
				if rf.CurrentTerm != args.Term {
					return
				}
				if reply.Term > rf.CurrentTerm {
					Debug(dLog, "Get Bigger Term when ask votes ,[%d] REVERT to follower", args.CandidateId)
					rf.status = Follower
					//rf.accumulatedHb++
					rf.CurrentTerm = reply.Term
					termNeedPersist = true
					return
				}
				var curV uint32
				if reply.VoteGranted {
					curV = atomic.AddUint32(&collectedVotes, 1)
					Debug(dVote, "[%d] GET vote from server[%d],cur votes :%d", args.CandidateId, i, curV)
				} else {
					Debug(dVote, "[%d] LOSS vote from server[%d]", args.CandidateId, i)
					return
				}
				if rf.status == Candidate && atomic.LoadUint32(&collectedVotes) >= uint32(succeedVoteNums) {
					rf.status = Leader
					Debug(dInfo, "[%d] Become leader in term %d", candidateId, rf.CurrentTerm)
					peerNums := len(rf.peers)
					for i := 0; i < len(rf.nextIndex); i++ {
						rf.nextIndex[i] = len(rf.Logs) + 1
					}
					go rf.Broadcast(candidateId, rf.CurrentTerm, peerNums, true)
					return
				}
			}(i)
		}
		time.Sleep(time.Duration(randomNumber) * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader || rf.status == Follower {
			rf.mu.Unlock()
			return
		}
		Debug(dTerm, "Candidate [%d] election time out Re election!", rf.me)
		rf.mu.Unlock()
		randomNumber = rand.Intn(401) + 400
	}

}

//func (rf *Raft) onElected(candidateId int, term int, succeedVoteNums int, collectedVotes uint32) {
//	rf.mu.Lock()
//	Debug(dLog, "Candidate [%d] election in term:%d need %d,get %d", candidateId, term, succeedVoteNums, collectedVotes)
//	// receive leaders heartbeat, revert to follower
//	if rf.status != Candidate || collectedVotes < uint32(succeedVoteNums) {
//		Debug(dLog, "Candidate [%d] fail elect,votes:%d,need:%d", candidateId, collectedVotes, succeedVoteNums)
//		return
//	}
//	rf.status = Leader
//	Debug(dInfo, "[%d] Become leader in term %d", candidateId, term)
//	reqs := rf.GenerateAppendReq(true, candidateId)
//	peerNums := len(rf.peers)
//	go rf.Broadcast(candidateId, reqs, peerNums)
//	rf.mu.Unlock()
//}

//
// example RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	termNeedPersist := false

	defer rf.needPersist(&termNeedPersist)

	me := rf.me
	Debug(dInfo, "[%d] PROCESS requestvote from [%d]", me, args.CandidateId)
	//reply.Term = rf.CurrentTerm

	// reject instantly
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm

		Debug(dInfo, "[%d] Receive vote request from [%d] lower term{%d}<curterm{%d}", me, args.CandidateId, args.Term, rf.CurrentTerm)
		return
	}

	// handle bigger term first
	if args.Term > rf.CurrentTerm {
		Debug(dInfo, " [%d] Receive vote request from [%d] higher term{%d}>curterm{%d} ", me, args.CandidateId, args.Term, rf.CurrentTerm)
		rf.CurrentTerm = args.Term
		termNeedPersist = true
		rf.status = Follower
	}

	// election vote check
	if !rf.CheckLatestLog(args) {
		Debug(dLog, "Candidate [%d]latest log check fail voter[%d]!", args.CandidateId, rf.me)
		reply.VoteGranted = false
		return
	}
	if _, ok := rf.VoteFor[rf.CurrentTerm]; ok {
		reply.VoteGranted = false
		Debug(dVote, "[%d] in term:%d already vote for [%d]", me, rf.CurrentTerm, rf.VoteFor[rf.CurrentTerm])
		return
	}

	rf.accumulatedHb++
	rf.VoteFor[rf.CurrentTerm] = args.CandidateId
	rf.persist()
	termNeedPersist = false // already persist below
	reply.VoteGranted = true
	rf.status = Follower

	Debug(dVote, "[%d] VOTE for [%d] in term %d", me, args.CandidateId, args.Term)
}

// need to be locked
func (rf *Raft) CheckLatestLog(args *RequestVoteArgs) bool {

	if len(rf.Logs) == 0 {
		Debug(dVote, "Candidate[%d] empty log checked by[%d]", args.CandidateId, rf.me)
		return true
	}
	Debug(dVote, "Candidate[%d]log checked by[%d] ,args:%+v, rf.latest term:%d,rf.latestindex:%d",
		args.CandidateId, rf.me, args, rf.Logs[len(rf.Logs)-1].Term, len(rf.Logs))
	if args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term {
		return true
	} else if args.LastLogTerm < rf.Logs[len(rf.Logs)-1].Term {
		return false
	} else {
		return args.LastLogIndex >= len(rf.Logs)
	}

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
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	Debug(dInfo, "[%d] ASK vote request to server[%d]", args.CandidateId, server)
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	if !ok {
		Debug(dError, "RPC REPLY ERROR [%d] ASK vote request to server[%d]", args.CandidateId, server)
		return false
	}
	return true
}

//func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
//	Debug(dInfo, "[%d] ASK vote request to server[%d]", args.CandidateId, server)
//	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
//	times := 0
//	for !ok && times < 100000 {
//		times++
//		rf.mu.Lock()
//		args.Term = rf.CurrentTerm
//		rf.mu.Unlock()
//		ok = rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
//		//Debug(dInfo, "Candidate [%d] Retry vote request to [%d]", rf.me, server)
//	}
//	Debug(dInfo, "[%d] FINISH vote request with server[%d] call times:[%d]", args.CandidateId, server, times)
//}
