package raft

//
// A Go object to append Logs
//
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type ReplyAppendEntries struct {
	Term      int
	Succeeded bool

	// Only used when prevlogcheck failed
	ConflictIndex int
	ConflictTerm  int
}

type RequestAppendEntries struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Logs              []LogEntry
	LeaderCommitIndex int
	IsHeartBeat       bool
}

func (rf *Raft) HandleAppendEntries(args *RequestAppendEntries, reply *ReplyAppendEntries) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dInfo, "[%d] RECEIVE APPEND RPC from leader [%d] me.term:%d, args:%+v", rf.me, args.LeaderID, rf.currentTerm, args)
	reply.Succeeded = true
	if args.Term < rf.currentTerm {
		reply.Succeeded = false
		reply.Term = rf.currentTerm
		Debug(dInfo, "[%d] REJECT APPEND RPC from lower leader [%d] in term:%d, me.term:%d", rf.me, args.LeaderID, args.Term, rf.currentTerm)
		return
	}

	if !rf.CheckPrevLog(args, reply) {
		Debug(dInfo, "[%d] CheckPrevLog from leader [%d] fail args:%+v", rf.me, args.LeaderID, args)
		reply.Succeeded = false
		//return
	}

	rf.status = Follower
	rf.accumulatedHb++
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if !reply.Succeeded {
		return
	}
	Debug(dInfo, "[%d] CheckPrevLog from leader [%d] args:%+v", rf.me, args.LeaderID, args)
	index := rf.checkIfConflict(args)
	if index == -1 {
		Debug(dInfo, "[%d] No conflict with append rpc from [%d]!", rf.me, args.LeaderID)
		// Not conflict ,don't change the log!
	} else {
		Debug(dLog, "[%d] log Conflict with append rpc IN index:%d! "+
			"from leader [%d],commit index:%d,prev log index:%d, "+
			"args.log.len:%d server logs.len:%d",
			rf.me, index, args.LeaderID, args.LeaderCommitIndex, args.PrevLogIndex, len(args.Logs), len(rf.logs))
		// conflict ,use rule 3 to delete all the log following
		logsCp := rf.logs[:index+args.PrevLogIndex]
		logsCp = append(logsCp, args.Logs[index:]...)
		rf.logs = logsCp
	}
	// rule 5 for append rpc
	if args.LeaderCommitIndex > rf.commitIndex {
		if args.LeaderCommitIndex > len(rf.logs) {
			rf.commitIndex = len(rf.logs)
		} else {
			rf.commitIndex = args.LeaderCommitIndex
		}
		rf.applyCommit()
	}

}

//Check if server has conflict log with leader sent
// rules 3 for append rpc
// return conflict index off in args.log
func (rf *Raft) checkIfConflict(args *RequestAppendEntries) int {
	argsLength := len(args.Logs)
	if argsLength == 0 {
		return -1
	}
	serverLogLength := len(rf.logs)
	for i := 0; i < argsLength; i++ {
		// exceed server's length
		if i+args.PrevLogIndex >= serverLogLength {
			return i
		}
		// conflict in i
		if rf.logs[i+args.PrevLogIndex].Term != args.Logs[i].Term {
			return i
		}
	}
	return -1
}

// if false return the conflict log index and term
func (rf *Raft) CheckPrevLog(args *RequestAppendEntries, reply *ReplyAppendEntries) bool {
	//if prevLogIndex <= 1 {
	//	return true
	//}
	if args.PrevLogIndex == 0 {
		return true
	}
	if len(rf.logs) < args.PrevLogIndex {
		Debug(dLog, "append rpc from leader:[%d] prevlogindex > server[%d] Logs length", args.LeaderID,
			rf.me)
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		return false
	}
	if rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		Debug(dLog, "Not Equal! leader:[%d] log[%d].Term=[%d], "+
			"server:[%d] log[%d].Term=[%d]",
			args.LeaderID, args.PrevLogIndex, args.PrevLogTerm,
			rf.me, args.PrevLogIndex, rf.logs[args.PrevLogIndex-1].Term)
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-1].Term
		for i, log := range rf.logs {
			if log.Term == reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		return false
	}
	return true
}

// need lock
func (rf *Raft) GenerateAppendReq(leaderId, peerId int, heartbeat bool) RequestAppendEntries {
	//rf.mu.Lock()
	var req RequestAppendEntries
	req.LeaderID = leaderId
	req.Term = rf.currentTerm
	req.Logs = []LogEntry{}
	//req.PrevLogIndex = rf.nextIndex[peerId] - 1
	//if req.PrevLogIndex > 0 {
	//	req.PrevLogTerm = rf.logs[req.PrevLogIndex-1].Term
	//	req.Logs = rf.logs[req.PrevLogIndex-1:]
	//}
	if rf.nextIndex[peerId] <= 1 {
		req.Logs = rf.logs
		req.PrevLogIndex = 0
		req.PrevLogTerm = 0

	} else {
		req.PrevLogIndex = rf.nextIndex[peerId] - 1
		req.PrevLogTerm = rf.logs[req.PrevLogIndex-1].Term
		req.Logs = rf.logs[req.PrevLogIndex:]
		if req.PrevLogIndex >= len(rf.logs) {
			req.Logs = []LogEntry{}
		}
	}
	req.IsHeartBeat = heartbeat
	req.LeaderCommitIndex = rf.commitIndex
	//rf.mu.Unlock()
	return req
}

func (rf *Raft) Broadcast(leaderId int, term int, peerNums int, heartbeat bool) {
	Debug(dLeader, "[%d] leader broadcast in term %d", leaderId, term)
	for i := 0; i < peerNums; i++ {
		if i == leaderId {
			continue
		}
		rf.mu.Lock()
		if rf.status != Leader {
			Debug(dLeader, "[%d] For Range No longer leader in broadcast to [%d] ,term %d", leaderId, i, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go func(i int) {
			for {
				reply := ReplyAppendEntries{}
				rf.mu.Lock()
				if rf.status != Leader {
					//Debug(dLeader, "[%d] No longer leader in broadcast to [%d],term %d", leaderId, i, rf.currentTerm)
					rf.mu.Unlock()
					return
				}
				args := rf.GenerateAppendReq(leaderId, i, heartbeat)
				rf.mu.Unlock()

				if ok := rf.SendAppendEntries(i, &args, &reply); !ok {
					//Debug(dLeader, "[%d] Send RPC RESPONSE ERROR,continue")
					continue
				}

				rf.mu.Lock()
				// old term rpc reply
				if rf.currentTerm != args.Term {
					Debug(dLeader, "[%d] leader term != args.term!,leader.term=%d,args.term=%d", leaderId, rf.currentTerm, args.Term)
					rf.mu.Unlock()
					return
				}
				// bigger term! return
				if reply.Term > rf.currentTerm {
					Debug(dLeader, "[%d] Get Bigger Term when broadcast heartbeat To [%d] , REVERT to follower", leaderId, i)

					rf.status = Follower
					rf.accumulatedHb++
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					return
				}
				if reply.Succeeded {
					Debug(dLeader, "[%d] success append to [%d] ,previndex %d ", leaderId, i, args.PrevLogIndex)
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Logs)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.UpdateCommitIndex()
					rf.mu.Unlock()
					return
				}
				rf.UpdateNextIndex(i, reply, leaderId, args)
				rf.mu.Unlock()
			}

		}(i)

	}
}

func (rf *Raft) UpdateNextIndex(peer int, reply ReplyAppendEntries, leaderId int, args RequestAppendEntries) {
	mark := -1
	Debug(dLeader, "leader[%d] Append RPC To [%d] return false,back off prevlogindex and term RETRY!", leaderId, peer)
	if reply.ConflictTerm >= 0 {
		for j, log := range args.Logs {
			if log.Term == reply.ConflictTerm {
				mark = j + 1
				break
			}
		}
	}
	rf.nextIndex[peer] = reply.ConflictIndex
	if mark >= 0 {
		rf.nextIndex[peer] = mark
	}
}

func (rf *Raft) SendAppendEntries(me int, args *RequestAppendEntries, reply *ReplyAppendEntries) bool {
	//Debug(dLeader, "[%d] Send Append RPC to [%d],"+
	//	"is heartbeat:%t prelogindex:%d,prelogterm:%d",
	//	args.LeaderID, me, args.IsHeartBeat, args.PrevLogIndex, args.PrevLogTerm)
	ok := rf.peers[me].Call("Raft.HandleAppendEntries", args, reply)
	//if !ok {
	//	Debug(dError, "[%d] Append RPC to [%d] error,cant receive response", args.LeaderID, me)
	//
	//}
	//Debug(dLeader, "[%d] Finish Append RPC to [%d]", args.LeaderID, me)
	return ok
}

func (rf *Raft) ProcessNewCommands(entry LogEntry) {
	rf.logs = append(rf.logs, entry)
	go rf.Broadcast(rf.me, rf.currentTerm, len(rf.peers), false)
}
