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
	randomNumber := rand.Intn(201) + 300

	for !rf.killed() {
		rf.mu.Lock()
		rf.currentTerm++
		candidateId := rf.me
		term := rf.currentTerm
		rf.voteFor[term] = candidateId
		peers := rf.peers
		serversNum := len(rf.peers)
		rf.status = Candidate
		succeedVoteNums := serversNum/2 + 1
		Debug(dInfo, "Candidate [%d] start a election in term %d", candidateId, term)
		rf.mu.Unlock()
		// server num must be odd, and ceil it

		var collectedVotes uint32 = 1
		for i, _ := range peers {
			if i == rf.me {
				continue
			}
			args := RequestVoteArgs{}
			args.CandidateId = candidateId
			args.Term = term
			reply := RequestVoteReply{}
			rf.mu.Lock()
			if rf.status != Candidate {
				Debug(dVote, "[%d] No longer candidate Stop Election,term:%d", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go func(i int) {
				ok := rf.SendRequestVote(i, &args, &reply)
				if !ok {
					return
				}
				if reply.Term > term {
					Debug(dLog, "Get Bigger Term when ask votes ,[%d] REVERT to follower", args.CandidateId)
					rf.RevertToFollower(reply.Term, false)
					return
				}
				var curV uint32
				if reply.VoteGranted {
					curV = atomic.AddUint32(&collectedVotes, 1)
					Debug(dVote, "[%d] GET vote from server[%d],cur votes :%d", args.CandidateId, i, curV)
				} else {
					Debug(dVote, "[%d] LOSS vote from server[%d]", args.CandidateId, i)
				}
				rf.mu.Lock()
				if rf.status == Candidate && atomic.LoadUint32(&collectedVotes) >= uint32(succeedVoteNums) {
					rf.status = Leader
					Debug(dInfo, "[%d] Become leader in term %d", candidateId, term)
					reqs := rf.GenerateAppendReq(true, candidateId)
					peerNums := len(rf.peers)
					go rf.Broadcast(candidateId, reqs, peerNums)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

			}(i)
		}
		time.Sleep(time.Duration(randomNumber) * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		randomNumber = rand.Intn(201) + 300
	}
	//reqGroup.Wait()
	//rf.onElected(candidateId, term, succeedVoteNums, collectedVotes)

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
	me := rf.me
	curTerm := rf.currentTerm
	rf.accumulatedHb++
	rf.mu.Unlock()
	Debug(dInfo, "[%d] PROCESS requestvote from [%d]", me, args.CandidateId)
	if args.Term < curTerm {
		reply.VoteGranted = false
		Debug(dInfo, "[%d] Receive vote request from [d] lower term{%d}<curterm{%d}", me, args.CandidateId, args.Term, curTerm)
		return
	}

	reply.Term = curTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > curTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		Debug(dInfo, " [%d] Receive vote request from [%d] higher term{%d}>curterm{%d} reverse to follower", me, args.CandidateId, args.Term, curTerm)

	}
	if !rf.CheckLatestLog(args.LastLogIndex, args.LastLogTerm) {
		Debug(dLog, "latest log check fail!")
		reply.VoteGranted = false
		return
	}
	if _, ok := rf.voteFor[rf.currentTerm]; ok {
		reply.VoteGranted = false
		Debug(dVote, "[%d] in term:%d already vote for [%d]", me, rf.currentTerm, rf.voteFor[curTerm])
		return
	}

	reply.VoteGranted = true
	rf.voteFor[rf.currentTerm] = args.CandidateId
	Debug(dVote, "[%d] VOTE for [%d] in term %d", me, args.CandidateId, args.Term)
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
	Debug(dInfo, "[%d] FINISH vote request to server[%d]", args.CandidateId, server)
	if !ok {
		Debug(dError, "RPC REPLY ERROR [%d] ASK vote request to server[%d]", args.CandidateId, server)
		return false
	}
	return true
}
