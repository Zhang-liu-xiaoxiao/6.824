package raft

//
// A Go object to append logs
//
type LogEntry struct {
	Term    int
	Index   int
	Content interface{}
}

type ReplyAppendEntries struct {
	Term      int
	Succeeded bool
}

type RequestAppendEntries struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
	IsHeartBeat       bool
}

func (rf *Raft) HandleAppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {
	rf.mu.Lock()
	curTerm := rf.currentTerm
	if args.Term >= curTerm {
		rf.status = Follower
		rf.accumulatedHb++
		rf.currentTerm = curTerm
		Debug(dInfo, "[%d] RECEIVE APPEND RPC from leader [%d] in term:%d, me.term:%d", rf.me, args.LeaderID, args.Term, curTerm)
	} else {
		Debug(dInfo, "[%d] REJECT APPEND RPC from lower leader [%d] in term:%d, me.term:%d", rf.me, args.LeaderID, args.Term, curTerm)

	}
	reply.Term = rf.currentTerm
	defer rf.mu.Unlock()
}

func (rf *Raft) GenerateAppendReq(isHeartBeat bool, leaderId int) []RequestAppendEntries {

	var reqs []RequestAppendEntries
	length := len(rf.peers)
	for i := 0; i < length; i++ {
		req := RequestAppendEntries{}
		if i == leaderId {
			req = RequestAppendEntries{
				Term:        rf.currentTerm,
				LeaderID:    leaderId,
				IsHeartBeat: isHeartBeat,
			}
		} else {
			req = RequestAppendEntries{
				Term:              rf.currentTerm,
				LeaderID:          leaderId,
				LeaderCommitIndex: rf.commitIndex,
				IsHeartBeat:       isHeartBeat,
			}
		}
		reqs = append(reqs, req)
	}
	return reqs
}

func (rf *Raft) Broadcast(leaderId int, args []RequestAppendEntries, peerNums int) {
	Debug(dLeader, "leader[%d] broadcast in term %d", leaderId, args[0].Term)
	for i := 0; i < peerNums; i++ {
		if i == leaderId {
			continue
		}
		reply := ReplyAppendEntries{}
		go func(i int) {
			if ok := rf.SendAppendEntries(i, &args[i], &reply); ok {
				if reply.Term > args[i].Term {
					Debug(dLeader, "Get Bigger Term when broadcast heartbeat ,[%d] REVERT to follower", leaderId)
					rf.RevertToFollower(reply.Term, false)
				}
			} else {
				return
			}
		}(i)

	}
}

func (rf *Raft) SendAppendEntries(me int, args *RequestAppendEntries, reply *ReplyAppendEntries) bool {
	Debug(dLeader, "[%d] Send Append RPC to [%d],is heartbeat:%t", args.LeaderID, me, args.IsHeartBeat)
	ok := rf.peers[me].Call("Raft.HandleAppendEntries", args, reply)
	Debug(dLeader, "[%d] Finish Append RPC to [%d]", args.LeaderID, me)
	if !ok {
		Debug(dError, "[%d] Append RPC to [%d] error,cant receive response", args.LeaderID, me)
	}
	return ok

}
