package raft

import "fmt"

type LogEntry struct {
	Command interface{}
	Term    int
	// Index   int
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("{Term:%v, Command:%v}", l.Term, l.Command)
}

type LogEntries []*LogEntry

func (rf *Raft) getLogIndex(index int) int { return index - rf.lastIncludedIndex }

func (rf *Raft) getLogByIndex(index int) *LogEntry {
	if index > rf.getLastLogIndex() {
		return nil
	}
	return rf.logs[rf.getLogIndex(index)]
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.logs) - 1
}

func (rf *Raft) getLastLog() *LogEntry { return rf.logs[rf.getLastLogIndex()] }

func (rf *Raft) setLogByIndex(index int, e *LogEntry) {
	rf.logs[rf.getLogIndex(index)] = e
}

func (rf *Raft) getEmptyLogs() LogEntries {
	return LogEntries{&LogEntry{Command: nil, Term: -1}}
}
