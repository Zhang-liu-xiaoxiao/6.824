package raft

func (rf *Raft) maxLogIndex() int {
	return rf.LastIncludedIndex + len(rf.Logs)
}

func (rf *Raft) log2sliceIndex(i int) int {
	ret := i - rf.LastIncludedIndex - 1
	//Debug(dLog2, "[%d] Convert %d to slice index:%d,rf.lastIncludeIndex:%d,cur log length:%d",
	//	rf.me, i, ret, rf.maxLogIndex(), len(rf.Logs))
	return ret
}

func (rf *Raft) slice2LogIndex(i int) int {
	ret := i + rf.LastIncludedIndex + 1
	//Debug(dLog2, "[%d] Convert %d to log index:%d,rf.lastIncludeIndex:%d,cur log length:%d",
	//	rf.me, i, ret, rf.maxLogIndex(), len(rf.Logs))
	return ret
}

func (rf *Raft) getLatestTerm() int {
	if len(rf.Logs) == 0 {
		return rf.LastIncludedTerm
	}
	return rf.Logs[rf.log2sliceIndex(rf.maxLogIndex())].Term
}

func (rf *Raft) getLogIndexTerm(index int) int {
	if a := rf.log2sliceIndex(index); a < 0 {
		return rf.LastIncludedTerm
	}
	return rf.Logs[rf.log2sliceIndex(index)].Term
}

func (rf *Raft) deleteSnappedLog(lastIncludedIndex int) {
	var logsNew []LogEntry
	logsNew = append(logsNew, rf.Logs[lastIncludedIndex-rf.LastIncludedIndex:]...)
	rf.Logs = logsNew
}
