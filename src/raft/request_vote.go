package raft

import (
	"time"

	log "github.com/sirupsen/logrus"
)

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
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.RequestCh <- Request{
		Name:  rpcMethodRequestVote,
		Args:  args,
		Reply: reply,
	}
	log.Debugf("[RequestVote] Server %v received args: %+v", rf.me, args)
	<-rf.RequestDone[RequestNameIDMapping[rpcMethodRequestVote]]
}

func (rf *Raft) handleRequestVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Infof("[handleRequestVoteRequest] Start: Server %v state: %v, commitIndex: %v, lastLogIndex: %v, currentTerm: %v, voteFor: %v,  args: %+v, timestamp: %v",
		rf.me, rf.state, rf.commitIndex, rf.logs.LastIndex(), rf.currentTerm, rf.votedFor, args, time.Now().UnixNano())

	reply.ServerID = rf.me
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// request is out of date
		return
	}
	rf.isTermOutdateAndUpdateState(args.Term)
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// Make sure candidate is as up to date as follower
		// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
		// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		if lastLog := rf.logs.GetLast(); args.LastLogTerm > lastLog.Term || args.LastLogTerm == lastLog.Term && args.LastLogIndex >= rf.logs.LastIndex() {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.resetElectionTimer()
		}
	}
	log.Infof("[handleRequestVoteRequest] Finish: Server %v state: %v, currentTerm: %v, voteFor: %v,  args: %+v,", rf.me, rf.state, rf.currentTerm, rf.votedFor, args)
}

func (rf *Raft) handleRequestVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Infof("[handleRequestVoteReply] Server %v received reply: %+v, voteNums: %v, currentTerm: %v", rf.me, reply, rf.voteNums, rf.currentTerm)
	if rf.isTermOutdateAndUpdateState(reply.Term) {
		return
	}

	if reply.VoteGranted {
		rf.voteNums++
	}
	log.Infof("[handleRequestVoteReply] Server %v voteNums: %v/%v, isMajorityNum: %v", rf.me, rf.voteNums, len(rf.peers), rf.isMajorityNum(rf.voteNums))

	if rf.isMajorityNum(rf.voteNums) {
		rf.updateState(Leader)
	}
}
