package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term              int
	ServerID          int
	LastIncludedIndex int
	ReplicatedIndex   int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.RequestCh <- Request{
		Name:  rpcMethodInstallSnapshot,
		Args:  args,
		Reply: reply,
	}
	<-rf.RequestDone[RequestNameIDMapping[rpcMethodInstallSnapshot]]
}

func (rf *Raft) handleInstallSnapshotRequest(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.ServerID = rf.me
	reply.ReplicatedIndex = rf.getLastLogIndex()
	reply.LastIncludedIndex = rf.lastApplied

	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// Leader who sends AppendEntries is out of term
		return
	}
	rf.isTermOutdateAndUpdateState(args.Term)

	if args.LastIncludedIndex >= rf.getLastLogIndex() {
		rf.logs = rf.getEmptyLogs()
	} else {
		tmpLogs := rf.getEmptyLogs()
		tmpLogs = append(tmpLogs, rf.logs[args.LastIncludedIndex+1:]...)
		rf.logs = tmpLogs
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()
	rf.persister.SaveSnapshot(args.Data)
	rf.installServerSnapshot(args.Data)
	rf.lastApplied = args.LastIncludedIndex
	reply.ReplicatedIndex = rf.getLastLogIndex()
	reply.LastIncludedIndex = rf.lastApplied
}

func (rf *Raft) handleInstallSnapshotReply(reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isTermOutdateAndUpdateState(reply.Term) {
		return
	}
	rf.nextIndex[reply.ServerID] = reply.ReplicatedIndex
	rf.matchIndex[reply.ServerID] = reply.LastIncludedIndex
}
