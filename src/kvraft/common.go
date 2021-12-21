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
	ErrOutOfDate   = "ErrOutOfDate"

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
	GetRequestUid() string
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
	RequestUid string
}

func (args PutAppendArgs) GetRequestUid() string { return args.RequestUid }

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestUid string
}

func (args GetArgs) GetRequestUid() string { return args.RequestUid }

type GetReply struct {
	Err   Err
	Value string
}

func FuncLatency(name string, start time.Time, args ...interface{}) {
	defer log.Infof("[Function Latency][%v] latency %.2fs. args: %+v", name, time.Since(start).Seconds(), args)
}

func isReplySuccess(err Err) bool {
	return err == OK || err == ""
}
