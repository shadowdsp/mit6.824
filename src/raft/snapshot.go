package raft

import log "github.com/sirupsen/logrus"

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
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.RequestCh <- Request{
		Name:  rpcMethodInstallSnapshot,
		Args:  args,
		Reply: reply,
	}
	<-rf.RequestDone
}

func (rf *Raft) handleInstallSnapshotRequest(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Infof("[handleInstallSnapshotRequest][Start] Server %v: currentTerm: %v, args: %+v", rf.me, rf.currentTerm, args)
	reply.Term = rf.currentTerm
	reply.ServerID = rf.me
	reply.LastIncludedIndex = rf.lastApplied

	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// Leader who sends AppendEntries is out of term
		return
	}
	rf.isTermOutdateAndUpdateState(args.Term)
	// recieved heartbeat, reset election timeout
	rf.resetElectionTimer()
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedIndex >= rf.getLastLogIndex() {
		rf.logs = LogEntries{&LogEntry{Command: nil, Term: args.LastIncludedTerm}}
	} else if args.LastIncludedTerm != rf.getLogByIndex(args.LastIncludedIndex).Term {
		rf.logs = LogEntries{&LogEntry{Command: nil, Term: args.LastIncludedTerm}}
	} else {
		tmpLogs := LogEntries{&LogEntry{Command: nil, Term: args.LastIncludedTerm}}
		tmpLogs = append(tmpLogs, rf.logs[rf.getLogIndex(args.LastIncludedIndex)+1:]...)
		rf.logs = tmpLogs
	}
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()
	rf.persister.SaveSnapshot(args.Data)
	rf.installServerSnapshot(args.Data)
	rf.lastApplied = args.LastIncludedIndex
	reply.LastIncludedIndex = args.LastIncludedIndex
	rf.apply()
	log.Infof("[handleInstallSnapshotRequest][Finish] Server %v: currentTerm: %v, lastIncludedIndex: %v, "+
		"lastIncludedTerm: %v, lastApplied: %v",
		rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.lastApplied)
}

func (rf *Raft) handleInstallSnapshotReply(reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Infof("[handleInstallSnapshotReply][Start] Server %v: Leader %v currentTerm: %v, reply: %+v",
		reply.ServerID, rf.me, rf.currentTerm, reply)
	if rf.isTermOutdateAndUpdateState(reply.Term) {
		return
	}
	rf.nextIndex[reply.ServerID] = reply.LastIncludedIndex + 1
	rf.matchIndex[reply.ServerID] = reply.LastIncludedIndex
	log.Infof("[handleInstallSnapshotReply][Finished] Server %v: Leader %v currentTerm: %v, "+
		"nextIndex %v, matchIndex %v, reply: %+v",
		reply.ServerID, rf.me, rf.currentTerm,
		rf.nextIndex[reply.ServerID], rf.matchIndex[reply.ServerID], reply)
}
