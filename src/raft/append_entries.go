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
	// is leader out of date
	OutOfDate bool
}

// AppendEntries AppendEntries RPC handler
// Invoked by leader to replicate log entries (§5.3); also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// If receive AppendEntries from leader:
	// 1. The server should become the follower, and stop leader election.
	// 2. Refresh heartbeat time.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Debugf("[AppendEntries] Before: Server %v state: %v, currentTerm: %v, prevLog: %+v, args: %+v", rf.me, rf.state, rf.currentTerm, rf.logs.Get(args.PrevLogIndex), args)

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ReplicatedIndex = 0
	reply.OutOfDate = false
	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// Leader who sends AppendEntries is out of term
		reply.Success = false
		reply.OutOfDate = true
		return
	}
	if args.Term >= rf.currentTerm && rf.state == Candidate {
		// received heartbeat, stop voting and change state to Follower
		rf.state = Follower
	}
	rf.checkTermOrUpdateState(args.Term)
	reply.Term = rf.currentTerm
	// recieved heartbeat, reset election timeout
	rf.resetElectionTimeout()

	// Rule 2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if prevLog := rf.logs.Get(args.PrevLogIndex); prevLog == nil || prevLog.Term != args.PrevLogTerm {
		log.Warnf("[AppendEntries] Failed to append entries to Server[%d], args: %+v, rf.logs: %+v", rf.me, args, rf.logs)
		reply.Success = false
		return
	}

	// Rule 3: If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	i := 0
	for ; i <= args.Logs.LastIndex(); i++ {
		// The log at prevLogIndex is the same as leader, we should check the logs after prevLogIndex
		index := i + args.PrevLogIndex + 1
		if index > rf.logs.LastIndex() {
			break
		}
		if entry := args.Logs.Get(i); rf.logs.Get(index).Term != entry.Term {
			rf.logs = rf.logs[:index]
			break
		}
	}

	// Rule 4: Append any new entries not already in the log
	for ; i <= args.Logs.LastIndex(); i++ {
		index := i + args.PrevLogIndex + 1
		if index > rf.logs.LastIndex() {
			rf.logs = append(rf.logs, args.Logs.Get(i))
		}
	}

	// Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.LastIndex())
	}
	reply.ReplicatedIndex = rf.logs.LastIndex()
	return
}
