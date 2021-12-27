package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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

	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// Leader who sends AppendEntries is out of term
		return
	}
	rf.isTermOutdateAndUpdateState(args.Term)

	// if args.LastIncludedIndex
}

func (rf *Raft) handleInstallSnapshotReply(reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isTermOutdateAndUpdateState(reply.Term)
}
