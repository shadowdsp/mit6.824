package raft

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type LogEntries []*LogEntry

func (le LogEntries) LastIndex() int { return len(le) - 1 }

func (le LogEntries) Get(i int) *LogEntry {
	if i <= le.LastIndex() {
		return le[i]
	}
	return nil
}

func (le LogEntries) GetLast() *LogEntry { return le.Get(le.LastIndex()) }

func (le LogEntries) Append(entry *LogEntry) LogEntries {
	return append(le, entry)
}
