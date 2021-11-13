package raft

import log "github.com/sirupsen/logrus"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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
	ServerID    int
	// is request term out of date
	OutOfDate bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Debugf("[RequestVote] Start: Server %v state: %v, currentTerm: %v, voteFor: %v,  args: %+v,", rf.me, rf.state, rf.currentTerm, rf.votedFor, args)

	reply.ServerID = rf.me
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	reply.OutOfDate = false
	if args.Term < rf.currentTerm {
		reply.OutOfDate = true
		return
	}
	rf.checkTermOrUpdateState(args.Term)
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// Make sure candidate is as up to date as follower
		// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
		// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		if commitLog := rf.logs.Get(rf.commitIndex); args.LastLogTerm > commitLog.Term || args.LastLogTerm == commitLog.Term && args.LastLogIndex >= rf.logs.LastIndex() {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.resetElectionTimeout()
		}
	}
	log.Debugf("[RequestVote] Finish: Server %v state: %v, currentTerm: %v, voteFor: %v,  args: %+v,", rf.me, rf.state, rf.currentTerm, rf.votedFor, args)
	return
}
