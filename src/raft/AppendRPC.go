package raft

import "sync"

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
	me := rf.me
	curTerm := rf.currentTerm
	rf.accumulatedHb++
	rf.mu.Unlock()
	Debug(dInfo, "[%d] RECEIVE APPEND RPC from leader [%d] in term:%d, me.term:%d", me, args.LeaderID, args.Term, curTerm)
	if args.Term < curTerm {
		reply.Term = curTerm
		return
	}
}

func (rf *Raft) Broadcast(leaderId int, args []RequestAppendEntries) {
	Debug(dLeader, "leader[%d] broadcast in term %d", leaderId, args[0].Term)
	waitGroup := sync.WaitGroup{}
	for i := 0; i < len(args); i++ {
		if i == leaderId {
			continue
		}
		waitGroup.Add(1)
		reply := ReplyAppendEntries{}
		go func(i int) {
			defer waitGroup.Done()
			if ok := rf.SendAppendEntries(i, &args[i], &reply); ok {
				if reply.Term > args[i].Term {
					Debug(dLeader, "Get Bigger Term when broadcast heartbeat ,[%d] REVERT to follower", leaderId)
					rf.RevertToFollower(reply.Term, false)
				}
			}
		}(i)

	}
	waitGroup.Wait()
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

func (rf *Raft) GenerateAppendReq(isHeartBeat bool, leaderId int) []RequestAppendEntries {

	reqs := make([]RequestAppendEntries, len(rf.peers)-1)
	for i := 0; i < len(reqs); i++ {
		if i == leaderId {
			continue
		}
		req := RequestAppendEntries{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LeaderCommitIndex: rf.commitIndex,
			IsHeartBeat:       isHeartBeat,
		}
		reqs = append(reqs, req)
	}
	return reqs
}
