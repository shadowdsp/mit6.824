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

// TODO:
// 1. 节点断连后领导选举飘了，有节点在 Start 接收到日志后，突然发生了领导切换，导致 matchIndex 的状态更新有问题
// 2. 如果跟随者崩溃或者运行缓慢，再或者网络丢包，领导人会不断的重复尝试附加日志条目 RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。

import (
	"context"
	"fmt"
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
	heartbeatInterval         = 120 * time.Millisecond
	electionTimeoutLowerBound = 400
	electionTimeoutUpperBound = 600
	rpcTimeoutLimit           = 300 * time.Millisecond
	checkAppliedInterval      = 150 * time.Millisecond
	replicateLogInterval      = 200 * time.Millisecond
	agreeLogInterval          = 150 * time.Millisecond
	rpcMethodAppendEntries    = "Raft.AppendEntries"
	rpcMethodRequestVote      = "Raft.RequestVote"
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
	// votedFor initial state is -1
	votedFor int
	// start from index 1, will append an emtpy log at first
	logs LogEntries

	// Volatile state on server
	commitIndex int
	lastApplied int

	// Volatile state on candidate
	receivedVote map[int]bool

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int

	// follower election timeout timestamp
	electionTimeoutAt time.Time
}

func (rf *Raft) isMajorityNum(num int) bool {
	return num > len(rf.peers)/2
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
	var state State
	rf.mu.Lock()
	state = rf.state
	rf.mu.Unlock()
	return state
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

func (rf *Raft) checkTermOrUpdateState(term int) bool {
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
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
	err := RpcCallWithTimeout(rf.peers[server], rpcMethodRequestVote, args, reply, rpcTimeoutLimit)
	if err != nil {
		return err
	}
	rf.mu.Lock()
	rf.checkTermOrUpdateState(reply.Term)
	rf.mu.Unlock()
	return nil
}

func (rf *Raft) sendAppendEntriesWithTimeout(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	err := RpcCallWithTimeout(rf.peers[server], rpcMethodAppendEntries, args, reply, rpcTimeoutLimit)
	if err != nil {
		return err
	}
	rf.mu.Lock()
	rf.checkTermOrUpdateState(reply.Term)
	rf.mu.Unlock()
	return nil
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
	index, term, isLeader := rf.logs.LastIndex()+1, rf.currentTerm, (rf.state == Leader)
	rf.mu.Unlock()
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	rf.logs = append(rf.logs, &LogEntry{Command: command, Term: term})
	rf.nextIndex[rf.me] = rf.logs.LastIndex() + 1
	rf.matchIndex[rf.me] = rf.logs.LastIndex()
	log.Infof("Leader[%v] nextIndex %v, matchIndex %v", rf.me, rf.nextIndex[rf.me], rf.matchIndex[rf.me])
	rf.mu.Unlock()
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
func (rf *Raft) isElectionTimeout(ctx context.Context) bool {
	if time.Now().After(rf.electionTimeoutAt) {
		log.Debugf("Server %v is election timeout, state %v, term %v, votedFor %+v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		return true
	}
	return false
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeoutAt = time.Now().Add(
		time.Millisecond * time.Duration(rand.Intn(electionTimeoutUpperBound-electionTimeoutLowerBound)+electionTimeoutLowerBound))
}

func (rf *Raft) tryConvertToCdd(ctx context.Context) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Follower {
		return
	}

	if rf.isElectionTimeout(ctx) {
		log.Debugf("[tryConvertToCdd] Server %v election timeout, state: %v, term: %v, votedFor: %v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		// If 1. not receieved heartbeat from leader before election timeout
		//    2. not vote for other candidate.
		// Become a candidate.
		rf.state = Candidate
	}
}

func (rf *Raft) initializeLeaderState() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.logs.LastIndex() + 1
		rf.matchIndex[i] = 0
		log.Warnf("[initializeLeaderState] server[%v]: nextIndex: %v", i, rf.nextIndex[i])
	}
}

func (rf *Raft) electLeader(ctx context.Context) {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	// Candidate start leader election.
	// 1. Increase currentTerm;
	rf.currentTerm++
	// 2. Vote for self;
	rf.votedFor = rf.me
	// 3. Reset election timer;
	rf.resetElectionTimeout()

	log.Debugf("[Candidate] Server %v start election, state: %v, term: %v", rf.me, rf.state, rf.currentTerm)
	rf.mu.Unlock()

	// 4. Send RequestVote RPC to other servers.
	successVoteNums := len(rf.peers)/2 + 1
	voteNums := 1
	go func(ctx context.Context) {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(serverID int) {
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
				err := rf.sendRequestVoteWithTimeout(serverID, args, reply)
				if err != nil {
					// log.Infof(err.Error())
					return
				}
				if reply.OutOfDate {
					return
				}

				if reply.VoteGranted {
					rf.mu.Lock()
					voteNums++
					log.Debugf("[Candidate] Server %v received vote from %v in term %v, currentVoteNums: %v, successVoteNums: %v", rf.me, reply.ServerID, rf.currentTerm, voteNums, successVoteNums)
					rf.mu.Unlock()
				}
			}(i)
		}
	}(ctx)

	for {
		time.Sleep(10 * time.Millisecond)

		isFinished := false
		rf.mu.Lock()
		// 5. Stop election if:
		//	  5.1. Election timeout;
		//	  5.2. Receive the most of votes from other servers;
		//    5.3. Once received RPC AppendEntry from leader, become follower.
		if rf.state == Candidate && voteNums >= successVoteNums {
			log.Infof("[Candidate] Server %v received the most vote, election success and become leader in term %v", rf.me, rf.currentTerm)
			// If election is successful
			rf.initializeLeaderState()
			isFinished = true
		} else if rf.state == Follower {
			log.Infof("[Candidate] Server %v received the heartbeat from other server, stop election and become follower in term %v", rf.me, rf.currentTerm)
			// If receive Heartbeat from leader
			isFinished = true
		} else if rf.isElectionTimeout(ctx) {
			log.Infof("[Candidate] Server %v election timeout in term %v", rf.me, rf.currentTerm)
			rf.state = Follower
			isFinished = true
		}
		rf.mu.Unlock()

		if isFinished {
			break
		}
	}
}

func (rf *Raft) leaderSendHeartbeat() error {
	for {
		time.Sleep(heartbeatInterval)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		wg := sync.WaitGroup{}
		wg.Add(len(rf.peers) - 1)

		heartbeatNums := 1
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
					PrevLogTerm:  rf.logs.GetLast().Term,
					PrevLogIndex: rf.logs.LastIndex(),
					Logs:         LogEntries{},
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				err := rf.sendAppendEntriesWithTimeout(serverID, args, reply)
				if err != nil {
					// log.Infof(err.Error())
					return
				}
				if reply.OutOfDate {
					return
				}
				rf.mu.Lock()
				if reply.Success {
					rf.matchIndex[serverID] = reply.ReplicatedIndex
					rf.nextIndex[serverID] = reply.ReplicatedIndex + 1
				}
				// if leader is not out of date, heartbeat is valid
				heartbeatNums++
				rf.mu.Unlock()
			}(i)
		}
		wg.Wait()

		rf.mu.Lock()
		heartbeatSuccessLimit := len(rf.peers)/2 + 1
		if heartbeatNums < heartbeatSuccessLimit {
			log.Infof("Leader %v sendHeartbeats failed: %v/%v, and become Follower", rf.me, heartbeatNums, heartbeatSuccessLimit)
			rf.state = Follower
		}
		rf.mu.Unlock()
	}
	return nil
}

func (rf *Raft) leaderReplicateLog() error {
	for {
		time.Sleep(replicateLogInterval)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			if len(rf.logs) < rf.nextIndex[i] {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			go func(serverID int) {
				for retry := 0; retry < 3; {
					rf.mu.Lock()
					prevLogIndex := rf.nextIndex[serverID] - 1
					if prevLogIndex <= 0 {
						log.Warnf("Follower[%v] nextIndex %v, matchIndex %v", serverID, rf.nextIndex[serverID], rf.matchIndex[serverID])
					}
					prevLogTerm := rf.logs.Get(prevLogIndex).Term
					logsToAppend := LogEntries{}
					for i := prevLogIndex + 1; i <= rf.logs.LastIndex(); i++ {
						logsToAppend = append(logsToAppend, rf.logs.Get(i))
					}
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogTerm:  prevLogTerm,
						PrevLogIndex: prevLogIndex,
						Logs:         logsToAppend,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					err := rf.sendAppendEntriesWithTimeout(serverID, args, reply)
					if err != nil {
						// log.Infof(err.Error())
						if _, ok := err.(*RpcCallTimeoutError); ok {
							retry++
						}
						continue
					}

					if reply.OutOfDate {
						rf.mu.Lock()
						rf.state = Follower
						rf.mu.Unlock()
						break
					}

					if reply.Success {
						// successfully
						rf.mu.Lock()
						rf.nextIndex[serverID] = reply.ReplicatedIndex + 1
						rf.matchIndex[serverID] = reply.ReplicatedIndex
						log.Warnf("Successfully append logs %v to server %v/%v, matchIndex: %v, nextIndex: %v", logsToAppend, serverID, len(rf.peers), rf.matchIndex[serverID], rf.nextIndex[serverID])
						rf.mu.Unlock()
						break
					} else {
						rf.mu.Lock()
						log.Warnf("Failed to append logs %v to server %v/%v, matchIndex: %v, nextIndex: %v", logsToAppend, serverID, len(rf.peers), rf.matchIndex[serverID], rf.nextIndex[serverID])
						rf.nextIndex[serverID] -= 1
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
	return nil
}

func (rf *Raft) leaderAgreeLog() error {
	for {
		time.Sleep(agreeLogInterval)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}

		updatedCommitIndex := rf.commitIndex
		for i := range rf.peers {
			matchIndex := rf.matchIndex[i]
			log.Debugf("server[%v]: nextIndex %v, matchIndex %v, commitIndex %v", i, rf.nextIndex[i], matchIndex, rf.commitIndex)
			if matchIndex <= rf.commitIndex {
				continue
			}
			count := 0
			for j := range rf.peers {
				if rf.matchIndex[j] >= matchIndex {
					count++
				}
			}
			if rf.isMajorityNum(count) && rf.logs.Get(matchIndex).Term == rf.currentTerm && matchIndex > updatedCommitIndex {
				log.Debugf("Leader updatedCommitIndex %v", updatedCommitIndex)
				updatedCommitIndex = matchIndex
			}
		}
		rf.commitIndex = updatedCommitIndex
		log.Debugf("[leaderAgreeLog] leader: %v, commitIndex: %v, term: %v, lastApplied: %v", rf.me, rf.commitIndex, rf.logs.Get(rf.commitIndex).Term, rf.lastApplied)
		rf.mu.Unlock()
	}
	return nil
}

func (rf *Raft) run(ctx context.Context) error {
	var prevState State = ""
	for {
		time.Sleep(10 * time.Millisecond)
		state := rf.getState()
		switch state {
		case Follower:
			rf.tryConvertToCdd(ctx)
			break
		case Candidate:
			rf.electLeader(ctx)
			break
		case Leader:
			if prevState == state {
				break
			}
			go rf.leaderSendHeartbeat()
			go rf.leaderReplicateLog()
			go rf.leaderAgreeLog()
			break
		default:
			panic(fmt.Sprintf("Server %v is in unknown state %v", rf.me, rf.state))
		}

		rf.mu.Lock()
		prevState = state
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyCommittedLog(ctx context.Context, applyCh chan ApplyMsg) error {
	for {
		time.Sleep(checkAppliedInterval)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.logs.Get(rf.lastApplied)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			log.Infof("[applyCh] Server[%v] start to apply log %+v, message %+v", rf.me, entry, applyMsg)
			applyCh <- applyMsg
		}
		rf.mu.Unlock()
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

		// volatile state on leaders
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	ctx := context.Background()
	go rf.run(ctx)
	go rf.applyCommittedLog(ctx, applyCh)

	return rf
}

func init() {
	log.SetOutput(os.Stdout)
	// Only log the warning severity or above.
	log.SetLevel(log.WarnLevel)
	// log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{
		// DisableColors: true,
		FullTimestamp: true,
	})
}
