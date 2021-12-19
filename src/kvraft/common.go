package kvraft

import (
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	RpcNameGet       = "KVServer.Get"
	RpcNamePutAppend = "KVServer.PutAppend"

	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	OpNameGet    = "Get"
	OpNamePut    = "Put"
	OpNameAppend = "Append"
)

var RpcNameMapping = map[string]int{
	RpcNameGet:       0,
	RpcNamePutAppend: 1,
}

type Request struct {
	Args  Args
	Reply Reply
}

type Args interface {
	GetRequestID() int
}

type Reply interface{}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestID int
}

func (args PutAppendArgs) GetRequestID() int { return args.RequestID }

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestID int
}

func (args GetArgs) GetRequestID() int { return args.RequestID }

type GetReply struct {
	Err   Err
	Value string
}

func FuncLatency(name string, start time.Time, args ...interface{}) {
	defer log.Infof("[Function Latency][%v] latency %.2fs. args: %+v", name, time.Since(start).Seconds(), args)
}
