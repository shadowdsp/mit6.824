package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"

	"net/http"
	_ "net/http/pprof"

	log "github.com/sirupsen/logrus"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type Request struct {
	Name  string
	Args  interface{}
	Reply interface{}
}

func (req Request) GetID() int {
	return RequestNameIDMapping[req.Name]
}

type Reply interface{}

const (
	heartbeatInterval         = 120 * time.Millisecond
	electionTimeoutLowerBound = 400
	electionTimeoutUpperBound = 600
	rpcTimeoutLimit           = 50 * time.Millisecond
	rpcMethodAppendEntries    = "Raft.AppendEntries"
	rpcMethodRequestVote      = "Raft.RequestVote"
	rpcMethodInstallSnapshot  = "Raft.InstallSnapshot"
)

type State string

var (
	Leader               = State("Leader")
	Candidate            = State("Candidate")
	Follower             = State("Follower")
	RequestNameIDMapping = map[string]int{
		rpcMethodAppendEntries: 0,
		rpcMethodRequestVote:   1,
	}
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 1 follower, 2 candidate, 3 leader
	state State

	// Persistent state on server
	currentTerm int
	// votedFor initial state is -1
	votedFor int
	voteNums int

	// start from index 1, will append an emtpy log at first
	logs LogEntries

	// Volatile state on server
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int

	// follower election timeout timestamp
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer

	// Request and Reply channel
	RequestCh   chan Request
	ReplyCh     chan Reply
	RequestDone []chan struct{}

	applyCh chan ApplyMsg
}

func (rf *Raft) isMajorityNum(num int) bool {
	return num*2 > len(rf.peers)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
}

func (rf *Raft) getState() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		log.Warnf("[persist] Failed to encode currentTerm: %v", err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Warnf("[persist] Failed to encode votedFor: %v", err)
	}
	if err := e.Encode(rf.logs); err != nil {
		log.Warnf("[persist] Failed to encode logs: %v", err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// log.Infof("[persist] Server[%v] persist state, term: %v, commitIndex: %v, log: %+v",
	// 	rf.me, rf.currentTerm, rf.commitIndex, rf.logs)
}

func (rf *Raft) EncodeSnapshot(v Snapshot) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(v); err != nil {
		log.Warnf("[EncodeSnapshot] Failed to encode: %v", err)
	}
	return w.Bytes()
}

func (rf *Raft) DecodeSnapshot(data []byte) Snapshot {
	snapshot := Snapshot{}

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return snapshot
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&snapshot); err != nil {
		log.Warnf("[DecodeSnapshot] Failed to decode snapshot: %v", err)
	}
	return snapshot
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs LogEntries

	decodeValues := func(values []interface{}) error {
		for _, v := range values {
			if err := d.Decode(v); err != nil {
				return err
			}
		}
		return nil
	}
	if err := decodeValues([]interface{}{&currentTerm, &votedFor, &logs}); err != nil {
		log.Warnf("[readPersist] failed to decode, error: %v", err)
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	log.Infof("[readPersist] Server[%v] read persist, term: %v, commitIndex: %v, log: %+v",
		rf.me, rf.currentTerm, rf.commitIndex, rf.logs)
}

func (rf *Raft) isTermOutdateAndUpdateState(term int) bool {
	if term > rf.currentTerm {
		rf.updateState(Follower)
		rf.votedFor = -1
		rf.voteNums = 0
		rf.currentTerm = term
		return true
	}
	return false
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVoteWithTimeout(server int, args *RequestVoteArgs, reply *RequestVoteReply) error {
	// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return RpcCallWithTimeout(rf.peers[server], rpcMethodRequestVote, args, reply, rpcTimeoutLimit)
}

func (rf *Raft) sendAppendEntriesWithTimeout(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return RpcCallWithTimeout(rf.peers[server], rpcMethodAppendEntries, args, reply, rpcTimeoutLimit)
}

func (rf *Raft) sendInstallSnapshotWithTimeout(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	return RpcCallWithTimeout(rf.peers[server], rpcMethodInstallSnapshot, args, reply, rpcTimeoutLimit)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index, term, isLeader := rf.logs.LastIndex()+1, rf.currentTerm, (rf.state == Leader)
	if !isLeader {
		return index, term, isLeader
	}

	entry := &LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
		Id:      rf.logs.LastId() + 1,
	}
	rf.logs = append(rf.logs, entry)
	rf.nextIndex[rf.me] = rf.logs.LastIndex() + 1
	rf.matchIndex[rf.me] = rf.logs.LastIndex()
	log.Infof("[Start] Leader %v start to append log[%v] %+v", rf.me, index, entry)
	return index, term, isLeader
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Store             map[string]string
}

func (rf *Raft) DoSnapshot(snapshot Snapshot) {
	rf.mu.Lock()

	snapshotData := rf.EncodeSnapshot(snapshot)
	rf.persister.SaveSnapshot(snapshotData)

	lastIncludedIndex := snapshot.LastIncludedIndex
	includedLog := rf.logs.GetByIndex(lastIncludedIndex)
	rf.logs = rf.logs[includedLog.Id+1:]

	state := rf.state
	rf.mu.Unlock()

	if state != Leader {
		return
	}

	// leader should send InstallSnapshot
	for serverID := range rf.peers {
		rf.mu.Lock()
		matchIndex := rf.matchIndex[serverID]
		if matchIndex < lastIncludedIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  snapshot.LastIncludedTerm,
				Data:              snapshotData,
			}
			reply := &InstallSnapshotReply{}
			rf.mu.Unlock()
			go func(serverID int) {
				if err := rf.sendInstallSnapshotWithTimeout(serverID, args, reply); err != nil {
					return
				}
				rf.ReplyCh <- reply
			}(serverID)
		} else {
			rf.mu.Unlock()
		}
	}
	return
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) cleanUpIfKilled() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Infof("Server %d was killed", rf.me)
	defer func() {
		if recover() != nil {
		}
	}()
	close(rf.applyCh)
	rf.stopTimer(rf.electionTimer)
	rf.stopTimer(rf.heartbeatTimer)
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(
		rand.Intn(electionTimeoutUpperBound-electionTimeoutLowerBound)+electionTimeoutLowerBound)
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(rf.getElectionTimeout())
}

func (rf *Raft) resetHeatbeatTimer() {
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(heartbeatInterval)
}

func (rf *Raft) stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (rf *Raft) electLeader() {
	// Candidate start leader election.
	rf.mu.Lock()
	// 1. Increase currentTerm;
	rf.currentTerm++
	// 2. Vote for self;
	rf.votedFor = rf.me
	rf.voteNums = 1
	// 3. Reset election timer;
	rf.resetElectionTimer()

	log.Debugf("[electLeader] Server %v start election, state: %v, term: %v", rf.me, rf.state, rf.currentTerm)
	rf.mu.Unlock()

	// 4. Send RequestVote RPC to other servers.
	for serverID := range rf.peers {
		if serverID == rf.me {
			continue
		}
		rf.mu.Lock()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.logs.LastIndex(),
			LastLogTerm:  rf.logs.GetLast().Term,
		}
		log.Debugf("[Candidate] Server %v sendRequestVote to server %d, args: %+v", rf.me, serverID, args)
		rf.mu.Unlock()
		reply := &RequestVoteReply{}

		go func(serverID int) {
			if err := rf.sendRequestVoteWithTimeout(serverID, args, reply); err != nil {
				return
			}
			rf.ReplyCh <- reply
		}(serverID)
	}
}

func (rf *Raft) sendHeartbeat() {
	for serverID := range rf.peers {
		if serverID == rf.me {
			continue
		}

		rf.mu.Lock()
		prevLogIndex := rf.nextIndex[serverID] - 1
		prevLog := rf.logs.GetByIndex(prevLogIndex)
		prevLogTerm := prevLog.Term
		logsToAppend := LogEntries{}
		for i := prevLog.Id + 1; i <= rf.logs.LastId(); i++ {
			logsToAppend = append(logsToAppend, rf.logs.GetById(i))
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Logs:         logsToAppend,
			LeaderCommit: rf.commitIndex,
		}
		log.Debugf("[heartbeat] Leader %v sendHeartbeat to server %v, prevLogIndex %v, prevLogTerm %v, len(logs) %v",
			rf.me, serverID, prevLogIndex, prevLogTerm, len(logsToAppend))
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		go func(serverID int) {
			if err := rf.sendAppendEntriesWithTimeout(serverID, args, reply); err != nil {
				return
			}
			rf.ReplyCh <- reply
		}(serverID)
	}
}

func (rf *Raft) updateState(state State) {
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(rf.getElectionTimeout())
	}
	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(heartbeatInterval)
	}
	if rf.state == state {
		return
	}

	log.Infof("[updateState] server %v term: %v, old state: %v, new state: %v", rf.me, rf.currentTerm, rf.state, state)
	if state == Leader {
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.logs.LastIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.resetHeatbeatTimer()
		rf.stopTimer(rf.electionTimer)
	} else if state == Candidate {
		rf.stopTimer(rf.heartbeatTimer)
	} else if state == Follower {
		rf.resetElectionTimer()
		rf.stopTimer(rf.heartbeatTimer)
	} else {
		log.Fatalf("[updateState] Unkown state: %v", state)
	}
	rf.state = state
}

func (rf *Raft) handleEvent() {
	for {
		if rf.killed() {
			rf.cleanUpIfKilled()
			return
		}
		select {
		case req := <-rf.RequestCh:
			switch req.Args.(type) {
			case *RequestVoteArgs:
				rf.handleRequestVoteRequest(req.Args.(*RequestVoteArgs), req.Reply.(*RequestVoteReply))
			case *AppendEntriesArgs:
				rf.handleAppendEntriesRequest(req.Args.(*AppendEntriesArgs), req.Reply.(*AppendEntriesReply))
			}
			rf.RequestDone[req.GetID()] <- struct{}{}
		case reply := <-rf.ReplyCh:
			switch reply.(type) {
			case *RequestVoteReply:
				rf.handleRequestVoteReply(reply.(*RequestVoteReply))
			case *AppendEntriesReply:
				rf.handleAppendEntriesReply(reply.(*AppendEntriesReply))
			}
		}
	}
}

func (rf *Raft) runTimerCron() {
	rf.mu.Lock()
	oldTerm := rf.currentTerm
	oldState := rf.state
	rf.mu.Unlock()
	for {
		if rf.killed() {
			rf.cleanUpIfKilled()
			return
		}
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.updateState(Candidate)
			rf.mu.Unlock()
			return
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state == Leader {
				rf.sendHeartbeat()
				rf.resetHeatbeatTimer()
			} else {
				return
			}
		}
		rf.mu.Lock()
		isSame := (oldTerm != rf.currentTerm || oldState != rf.state)
		rf.mu.Unlock()
		if !isSame {
			return
		}
	}
}

func (rf *Raft) run() {
	for {
		if rf.killed() {
			log.Infof("Server %d was killed", rf.me)
			time.Sleep(heartbeatInterval)
			return
		}
		state := rf.getState()
		rf.mu.Lock()
		log.Debugf("[run] Server %d term: %v, state: %v, timestamp: %v", rf.me, rf.currentTerm, state, time.Now().UnixNano())
		rf.mu.Unlock()
		switch state {
		case Candidate:
			rf.electLeader()
			break
		case Leader:
			rf.sendHeartbeat()
			break
		}
		rf.runTimerCron()
	}
}

func (rf *Raft) apply() {
	for rf.lastApplied < rf.commitIndex {
		index := rf.lastApplied + 1
		entry := rf.logs.GetByIndex(index)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: index,
			CommandTerm:  entry.Term,
		}
		log.Infof("[applyCh] Server %v start to apply log %+v, message %+v", rf.me, entry, applyMsg)
		rf.applyCh <- applyMsg
		rf.lastApplied++
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       Follower,
		votedFor:    -1,
		logs:        LogEntries{&LogEntry{Command: nil, Term: -1}},
		currentTerm: 0,

		// volatile state on servers
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     applyCh,

		// volatile state on leaders
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		RequestCh:   make(chan Request),
		ReplyCh:     make(chan Reply),
		RequestDone: make([]chan struct{}, len(RequestNameIDMapping)),
	}
	for i := range rf.RequestDone {
		rf.RequestDone[i] = make(chan struct{})
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.updateState(Follower)
	go rf.run()
	go rf.handleEvent()

	return rf
}

func startPprofServer() {
	http.ListenAndServe("0.0.0.0:6060", nil)
}

func init() {
	log.SetOutput(os.Stdout)
	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
	log.SetLevel(log.InfoLevel)
	log.SetLevel(log.WarnLevel)
	log.SetFormatter(&log.TextFormatter{
		// DisableColors: true,
		FullTimestamp: true,
	})

	// go startPprofServer()
}
