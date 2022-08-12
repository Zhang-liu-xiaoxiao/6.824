package raft

type InstallSnapShotReq struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotResp struct {
	Term int
}

func (rf *Raft) HandleInstallSnapRPC(req *InstallSnapShotReq, resp *InstallSnapShotResp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "[%d] RECEIVE Install SNAP RPC from leader [%d],"+
		" me.term:%d, reqs:%+v", rf.me, req.LeaderId, rf.CurrentTerm, req)
	if req.Term < rf.CurrentTerm {
		resp.Term = rf.CurrentTerm
		Debug(dSnap, "[%d] REJECT Install SNAP RPC from lower leader [%d],"+
			"in term:%d, me.term:%d", rf.me, req.LeaderId, req.Term, rf.CurrentTerm)
		return
	}
	rf.status = Follower
	rf.accumulatedHb++
	rf.CurrentTerm = req.Term
	resp.Term = rf.CurrentTerm

	if index := rf.checkIsExistSnapshot(req); index != -1 {
		// req is index of the committed log
		Debug(dSnap, "[%d] discard preceding , exist in :%d "+
			"rf.logs:%+v", rf.me, index, rf.Logs)

		if rf.LastIncludedIndex >= req.LastIncludedIndex {
			Debug(dSnap, "[%d] last included index:%d "+
				"> leader send install rpc:%d ", rf.me, rf.LastIncludedIndex, req.LastIncludedIndex)
			return
		}
		rf.LastIncludedIndex = req.LastIncludedIndex
		rf.LastIncludedTerm = req.LastIncludedTerm

		var logsCp []LogEntry
		logsCp = append(logsCp, rf.Logs[index+1:]...)
		rf.Logs = logsCp
		if rf.commitIndex < req.LastIncludedIndex {
			rf.commitIndex = req.LastIncludedIndex
		}
		if rf.lastApplied < req.LastIncludedIndex {
			rf.lastApplied = req.LastIncludedIndex
		}
		rf.persister.SaveStateAndSnapshot(nil, req.Data)
		rf.persist()
		//return
	} else {
		rf.Logs = []LogEntry{}
		rf.LastIncludedIndex = req.LastIncludedIndex
		rf.LastIncludedTerm = req.LastIncludedTerm
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      req.Data,
			SnapshotIndex: req.LastIncludedIndex,
			SnapshotTerm:  req.LastIncludedTerm,
		}
		rf.commitIndex = req.LastIncludedIndex
		rf.lastApplied = req.LastIncludedIndex
		Debug(dSnap, "[%d] discard all log,update Applied and Commit index to :%d", rf.me, req.LastIncludedIndex)
		rf.applyCh <- msg
	}
}

func (rf *Raft) checkIsExistSnapshot(req *InstallSnapShotReq) int {
	for i, log := range rf.Logs {
		if log.Index == req.LastIncludedIndex && log.Term == req.LastIncludedTerm {
			return i
		}
	}
	return -1
}

func (rf *Raft) CallSnapshotRPC(me int, args *InstallSnapShotReq, reply *InstallSnapShotResp) bool {
	ok := rf.peers[me].Call("Raft.HandleInstallSnapRPC", args, reply)
	return ok
}

func (rf *Raft) processSnapshotRPCReply(i int, req InstallSnapShotReq, leaderId int, resp InstallSnapShotResp, termNeedPersist *bool) {
	// old term rpc reply
	if rf.CurrentTerm != req.Term {
		Debug(dLeader, "[%d] leader term != args.term!,leader.term=%d,args.term=%d", leaderId, rf.CurrentTerm, req.Term)
	}
	// bigger term! return
	if resp.Term > rf.CurrentTerm {
		Debug(dLeader, "[%d] Get Bigger Term when broadcast heartbeat To [%d] , REVERT to follower", leaderId, i)

		rf.status = Follower
		rf.accumulatedHb++
		*termNeedPersist = true
	}
	rf.matchIndex[i] = req.LastIncludedIndex
	rf.nextIndex[i] = req.LastIncludedIndex + 1
}
