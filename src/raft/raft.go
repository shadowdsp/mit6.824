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
	"context"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"mit6.824/src/labrpc"

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
}

const (
	heartbeatTimeoutLimit     = 1 * time.Second
	heartbeatInterval         = 200 * time.Millisecond
	electionInterval          = 500 * time.Millisecond
	electionTimeoutLowerBound = 300
	electionTimeoutUpperBound = 600
)

type State string

var (
	Leader    = State("Leader")
	Candidate = State("Candidate")
	Follower  = State("Follower")
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
	votedFor    *int
	logs        []*LogEntry

	// Volatile state on server
	commitIndex int
	lastApplied int

	// Volatile state on candidate
	receivedVote map[int]bool

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int

	// the last heartbeat time received from leader
	lastHeartbeatTime time.Time

	// electionTimer
	electionChan chan struct{}
}

type LogEntry struct {
	Content string
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

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

// AppendEntriesArgs RPC argument structure
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []*LogEntry
	LeaderCommit int
	ServerID     int
}

// AppendEntriesReply RPC reply structure
type AppendEntriesReply struct {
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

func (rf *Raft) checkTermOrUpdateState(term int) {
	if term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = term
		// need this ?
		rf.votedFor = nil
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Debugf("[RequestVote] Log1 Server %v state: %v, currentTerm: %v, voteFor: %v, log size: %v,  args: %+v,", rf.me, rf.state, rf.currentTerm, rf.votedFor, len(rf.logs), args)

	reply.ServerID = rf.me
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		// rf.state = Follower
		// rf.votedFor = nil
	}
	rf.checkTermOrUpdateState(args.Term)
	reply.Term = rf.currentTerm
	log.Debugf("[RequestVote] Log2 Server %v state: %v, currentTerm: %v, voteFor: %v, log size: %v,  args: %+v,", rf.me, rf.state, rf.currentTerm, rf.votedFor, len(rf.logs), args)

	if rf.votedFor == nil || *rf.votedFor == args.CandidateID {
		// Make sure candidate is as up to date as follower
		log.Debugf("[RequestVote] Server %v log size: %v", rf.me, len(rf.logs))
		if len(rf.logs) <= 0 || (args.LastLogIndex <= len(rf.logs)-1 && args.LastLogTerm <= rf.logs[len(rf.logs)-1].Term) {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateID
		}
	}
	return
}

// AppendEntries AppendEntries RPC handler
// Invoked by leader to replicate log entries (§5.3); also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// If receive AppendEntries from leader:
	// 1. The server should become the follower, and stop leader election.
	// 2. Refresh heartbeat time.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Debugf("[AppendEntries] Before: Server %v state: %v, currentTerm: %v, log size: %v,  args: %+v, lastHeartbeat: %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), args, rf.lastHeartbeatTime)

	if args.Term < rf.currentTerm {
		// Important!!!
		return
	}
	if args.Term >= rf.currentTerm && rf.state == Candidate {
		rf.state = Follower
	}
	rf.checkTermOrUpdateState(args.Term)
	// if args.Term > rf.currentTerm {
	// 	rf.currentTerm = args.Term
	// 	rf.state = Follower
	// }
	rf.lastHeartbeatTime = time.Now()
	log.Debugf("[AppendEntries] After: Server %v state: %v, currentTerm: %v, log size: %v,  args: %+v, lastHeartbeat: %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), args, rf.lastHeartbeatTime)
	return

	// // TODO: We need a more efficient data-structrue to maintain logs
	// reply.Term = rf.currentTerm
	// reply.Success = true

	// // Rule 1: Reply false if term < currentTerm
	// if args.Term < rf.currentTerm {
	// 	reply.Success = false
	// 	return
	// }

	// // Rule 2: Reply false if log doesn’t contain an entry at prevLogIndex
	// // whose term matches prevLogTerm
	// exist := false
	// for _, entry := range rf.logs {
	// 	if entry.Term == args.PrevLogTerm && entry.Index == args.PrevLogIndex {
	// 		exist = true
	// 	}
	// }
	// if !exist {
	// 	reply.Success = false
	// 	return
	// }

	// // Rule 3: If an existing entry conflicts with a new one (same index but different terms),
	// // delete the existing entry and all that follow it
	// deleteIndex := -1
	// for i, entry := range rf.logs {
	// 	for _, nEntry := range args.Logs {
	// 		if entry.Index == nEntry.Index && entry.Term != nEntry.Term {
	// 			deleteIndex = i
	// 			break
	// 		}
	// 	}
	// 	if deleteIndex != -1 {
	// 		break
	// 	}
	// }
	// if deleteIndex != -1 {
	// 	rf.logs = rf.logs[:deleteIndex]
	// }

	// // Rule 4: Append any new entries not already in the log
	// for _, nEntry := args.Logs {
	// 	exist := false
	// 	for _, entry := range rf.logs {
	// 		if nEntry.isSame(entry) {
	// 			exist = true
	// 		}
	// 	}
	// 	if !exist {
	// 		rf.logs = append(rf.logs, nEntry)
	// 	}
	// }

	// // Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// if args.LeaderCommit > rf.commitIndex {
	// 	rf.commitIndex = min(args.Leadercommit, rf.logs[len(rf.logs) - 1].Index)
	// }
	// return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	rf.checkTermOrUpdateState(reply.Term)
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	rf.checkTermOrUpdateState(reply.Term)
	rf.mu.Unlock()
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

// Customized functions
func (rf *Raft) isHeartbeatTimeout(ctx context.Context) bool {
	if time.Now().After(rf.lastHeartbeatTime.Add(heartbeatTimeoutLimit)) {
		log.Debugf("Server %v is HeartbeatTimeout, state %v, term %v, votedFor %+v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		return true
	}
	return false
}

func (rf *Raft) getRandomElectionTimeout() time.Time {
	return time.Now().Add(time.Millisecond * time.Duration(rand.Intn(electionTimeoutUpperBound-electionTimeoutLowerBound)+electionTimeoutLowerBound))
}

func (rf *Raft) checkHeartbeatTimeout(ctx context.Context) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

}

func (rf *Raft) checkHeartbeatTimeoutOrElection(ctx context.Context) {
	defer log.Debugf("[checkHeartbeatTimeoutOrElection] Server %v finished", rf.me)

	// If 1. not receieved heartbeat from leader for a while,
	//    2. not vote for other candidate.
	// Become a candidate.
	rf.mu.Lock()
	log.Debugf("[checkHeartbeatTimeoutOrElection] Server %v start, state: %v, term: %v, votedFor: %v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	if rf.state == Follower && rf.isHeartbeatTimeout(ctx) && rf.votedFor == nil {
		rf.state = Candidate
	}
	// Here can resolve the case that leader has sent AppendEntries
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	// Candidate start leader election.
	// 1. Increase currentTerm;
	rf.currentTerm++
	// 2. Vote for self;
	rf.votedFor = &rf.me

	log.Debugf("[checkHeartbeatTimeoutOrElection] Server %v start election, state: %v, term: %v", rf.me, rf.state, rf.currentTerm)
	rf.mu.Unlock()

	// 3. Reset election timer;
	electTimeoutDuration := rf.getRandomElectionTimeout()

	// 4. Send RequestVote RPC to other servers.
	//	  4.1. Election timeout;
	//	  4.2. Receive the most of votes from other servers;
	//    4.3. Once received RPC AppendEntry from leader, become follower.
	// No +1 because self vote for self, excceed half
	successVoteNums := len(rf.peers) / 2
	voteNums := 0
	go func(ctx context.Context) {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(serverID int) {
				rf.mu.Lock()
				args := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateID: rf.me,
					// LastLogIndex: len(rf.logs) - 1,
					// LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
				}
				log.Debugf("[checkHeartbeatTimeoutOrElection] Server %v sendRequestVote to server %d, args: %+v", rf.me, serverID, args)
				rf.mu.Unlock()

				reply := &RequestVoteReply{}
				rf.sendRequestVote(serverID, args, reply)

				if reply.VoteGranted {
					rf.mu.Lock()
					voteNums++
					log.Debugf("[checkHeartbeatTimeoutOrElection] Server %v received vote from %v in term %v, currentVoteNums: %v, successVoteNums: %v", rf.me, reply.ServerID, rf.currentTerm, voteNums, successVoteNums)
					rf.mu.Unlock()
				}
			}(i)
		}
	}(ctx)

	electionSuccess := false
	for {
		time.Sleep(10 * time.Millisecond)

		isFinished := false
		rf.mu.Lock()
		if voteNums >= successVoteNums {
			log.Infof("[checkHeartbeatTimeoutOrElection] Server %v received the most vote, and become leader in term %v", rf.me, rf.currentTerm)
			// If election is successful
			rf.state = Leader
			electionSuccess = true
			isFinished = true
		} else if rf.state == Follower {
			log.Infof("[checkHeartbeatTimeoutOrElection] Server %v received the heartbeat from other server, and become follower in term %v", rf.me, rf.currentTerm)
			// If receive Heartbeat from leader
			isFinished = true
		} else if time.Now().After(electTimeoutDuration) {
			log.Infof("[checkHeartbeatTimeoutOrElection] Server %v election timeout in term %v", rf.me, rf.currentTerm)
			rf.state = Follower
			rf.votedFor = nil
			isFinished = true
		}
		rf.mu.Unlock()

		if isFinished {
			break
		}
	}

	if electionSuccess {
		// TODO: Use concurrency strategy and
		// move it to another goroutine in other method is better.
		rf.sendHeartbeat(ctx)
	}
}

func (rf *Raft) sendHeartbeat(ctx context.Context) {
	_, isleader := rf.GetState()
	if isleader {
		wg := sync.WaitGroup{}
		wg.Add(len(rf.peers) - 1)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(serverID int) {
				defer wg.Done()
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogTerm:  1,
					PrevLogIndex: 1,
					Logs:         nil,
					LeaderCommit: 1,
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(serverID, args, reply)
			}(i)
		}
		wg.Wait()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.logs = make([]*LogEntry, 0)
	rf.state = Follower

	ctx := context.Background()
	// Your initialization code here (2A, 2B, 2C).
	go func(ctx context.Context) {
		// check heartbeat timeout or election
		electionTicker := time.NewTicker(electionInterval)
		for {
			// Every 500ms checkHeartbeatTimeoutOrElection
			select {
			case <-electionTicker.C:
				rf.checkHeartbeatTimeoutOrElection(ctx)
			case <-ctx.Done():
				log.Warnf("Election context done !")
			}
		}
	}(ctx)

	go func(ctx context.Context) {
		heartbeatTicker := time.NewTicker(heartbeatInterval)
		for {
			// Every 200ms checkHeartbeatTimeoutOrElection
			select {
			case <-heartbeatTicker.C:
				rf.sendHeartbeat(ctx)
			case <-ctx.Done():
				log.Warnf("Heartbeat context done !")
			}
		}
	}(ctx)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func init() {
	log.SetOutput(os.Stdout)
	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
	// log.SetLevel(log.WarnLevel)
	log.SetFormatter(&log.TextFormatter{
		// DisableColors: true,
		FullTimestamp: true,
	})
}
