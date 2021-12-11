package raft

import log "github.com/sirupsen/logrus"

// AppendEntriesArgs RPC argument structure
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         LogEntries
	LeaderCommit int
	ServerID     int
}

// AppendEntriesReply RPC reply structure
type AppendEntriesReply struct {
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
	// replication index, to update leader matchIndex
	ReplicatedIndex int
	// ServerID
	ServerID int
	// log inconsistency
	LogInconsistent bool
}

// AppendEntries AppendEntries RPC handler
// Invoked by leader to replicate log entries (§5.3); also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.RequestCh <- Request{
		Name:  rpcMethodAppendEntries,
		Args:  args,
		Reply: reply,
	}
	<-rf.RequestDone[RequestNameIDMapping[rpcMethodAppendEntries]]
}

func (rf *Raft) handleAppendEntriesRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// If receive AppendEntries from leader:
	// 1. The server should become the follower, and stop leader election.
	// 2. Refresh heartbeat time.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Debugf("[handleAppendEntriesRequest] Before: Server %v state: %v, currentTerm: %v, prevLog: %+v, args: %+v",
		rf.me, rf.state, rf.currentTerm, rf.logs.Get(args.PrevLogIndex), args)

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ReplicatedIndex = 0
	reply.ServerID = rf.me
	reply.LogInconsistent = false
	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// Leader who sends AppendEntries is out of term
		reply.Success = false
		return
	}
	rf.isTermOutdateAndUpdateState(args.Term)
	reply.Term = rf.currentTerm
	// recieved heartbeat, reset election timeout
	rf.resetElectionTimer()

	// Rule 2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if prevLog := rf.logs.Get(args.PrevLogIndex); prevLog == nil || prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		reply.LogInconsistent = true
		return
	}

	// Rule 3: If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// Rule 4: Append any new entries not already in the log
	for i := range args.Logs {
		// The log at prevLogIndex is the same as leader, we should check the logs after prevLogIndex
		index := i + args.PrevLogIndex + 1
		if entry := rf.logs.Get(index); entry != nil && entry.Term != args.Logs.Get(i).Term {
			rf.logs = rf.logs[:index]
		}
		if index > rf.logs.LastIndex() {
			rf.logs = append(rf.logs, args.Logs.Get(i))
		}
	}

	// Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.LastIndex())
	}
	rf.apply()

	reply.ReplicatedIndex = args.PrevLogIndex + len(args.Logs)
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isTermOutdateAndUpdateState(reply.Term) {
		return
	}

	if !reply.Success {
		if reply.LogInconsistent {
			if rf.nextIndex[reply.ServerID] > 0 {
				rf.nextIndex[reply.ServerID]--
			} else {
				log.Infof("[handleAppendEntriesReply] Abnormal nextIndex, Leader %v, Server %v, reply %+v", rf.me, reply.ServerID, reply)
			}
		}
		return
	}
	rf.nextIndex[reply.ServerID] = reply.ReplicatedIndex + 1
	rf.matchIndex[reply.ServerID] = reply.ReplicatedIndex

	// Try to update leader commit index
	matchIndex := rf.matchIndex[reply.ServerID]
	log.Debugf("[handleAppendEntriesReply] Server %v: nextIndex %v, matchIndex %v, commitIndex %v",
		reply.ServerID, rf.nextIndex[reply.ServerID], matchIndex, rf.commitIndex)
	count := 0
	for j := range rf.peers {
		if rf.matchIndex[j] >= matchIndex {
			count++
		}
	}
	if rf.isMajorityNum(count) && rf.logs.Get(matchIndex).Term == rf.currentTerm && matchIndex > rf.commitIndex {
		log.Debugf("Leader updatedCommitIndex %v", matchIndex)
		rf.commitIndex = matchIndex
	}
	rf.apply()
}
