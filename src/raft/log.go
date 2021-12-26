package raft

import "fmt"

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
	Id      int
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("{Term:%v, Index:%v, Command:%v, Id:%v}", l.Term, l.Index, l.Command, l.Id)
}

type LogEntries []*LogEntry

func (le LogEntries) LastIndex() int { return le.GetLast().Index }

func (le LogEntries) LastId() int { return le.GetLast().Id }

func (le LogEntries) GetById(i int) *LogEntry {
	if 0 <= i && i <= le.LastIndex() {
		return le[i]
	}
	return nil
}

func (le LogEntries) GetByIndex(index int) *LogEntry {
	for _, e := range le {
		if e.Index == index {
			return e
		}
	}
	return nil
}

func (le LogEntries) GetLast() *LogEntry { return le[len(le)-1] }
