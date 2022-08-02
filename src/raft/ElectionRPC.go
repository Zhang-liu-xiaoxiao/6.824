package raft

import (
	"sync"
	"sync/atomic"
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
	//rf.mu.Unlock()
	// server num must be odd, and ceil it
	voteGroup := sync.WaitGroup{}
	var collectedVotes uint32 = 1
	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{}
		args.CandidateId = candidateId
		args.Term = term
		reply := RequestVoteReply{}
		voteGroup.Add(1)
		go func(i int) {
			ok := rf.SendRequestVote(i, &args, &reply)
			defer voteGroup.Done()
			if !ok {
				return
			}
			if reply.Term > term {
				Debug(dLog, "Get Bigger Term when ask votes ,[%d] REVERT to follower", args.CandidateId)
				rf.RevertToFollower(reply.Term, false)
				return
			}
			if reply.VoteGranted {
				curV := atomic.AddUint32(&collectedVotes, 1)
				Debug(dVote, "[%d] GET vote from server[%d],cur votes :%d", args.CandidateId, i, curV)
			} else {
				Debug(dVote, "[%d] LOSS vote from server[%d]", args.CandidateId, i)
			}

		}(i)
	}
	voteGroup.Wait()
	//rf.mu.Lock()
	Debug(dLog, "Candidate [%d] election in term:%d need %d,get %d", candidateId, term, succeedVoteNums, collectedVotes)
	// receive leaders heartbeat, revert to follower
	if rf.status != Candidate || collectedVotes < uint32(succeedVoteNums) {
		Debug(dLog, "Candidate [%d] fail elect,votes:%d,need:%d,status:%d", collectedVotes, succeedVoteNums)
		return
	}
	rf.status = Leader
	Debug(dInfo, "[%d] Become leader in term %d", candidateId, term)
	//reqs := rf.GenerateAppendReq(true,candidateId)
	//reqs := make([]RequestAppendEntries, len(rf.peers)-1)
	var reqs []RequestAppendEntries
	for i := 0; i < serversNum; i++ {
		req := RequestAppendEntries{
			Term:        term,
			LeaderID:    candidateId,
			IsHeartBeat: true,
		}
		reqs = append(reqs, req)
	}
	//for i := 0; i < len(reqs); i++ {
	//	if i == candidateId {
	//		continue
	//	}
	//	req := RequestAppendEntries{
	//		Term:              rf.currentTerm,
	//		LeaderID:          rf.me,
	//		LeaderCommitIndex: rf.commitIndex,
	//		IsHeartBeat:       true,
	//	}
	//	reqs = append(reqs, req)
	//}
	Debug(dTest, "[%d] Become leader in term %d", candidateId, term)
	go rf.Broadcast(candidateId, reqs)
	rf.mu.Unlock()

}

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
	reply.Term = curTerm
	if args.Term < curTerm {
		reply.VoteGranted = false
		return
	}

	if !rf.CheckLatestLog(args.LastLogIndex, args.LastLogTerm) {
		Debug(dLog, "latest log check fail!")
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > curTerm {
		rf.currentTerm = args.Term
	}
	if _, ok := rf.voteFor[curTerm]; ok {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.voteFor[curTerm] = args.CandidateId
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
