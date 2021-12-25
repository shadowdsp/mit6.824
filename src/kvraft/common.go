package kvraft

import (
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	RpcNameKVRequest = "KVServer.KVRequest"

	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	OpNameGet    = "Get"
	OpNamePut    = "Put"
	OpNameAppend = "Append"
)

type Request struct {
	Args  *Args
	Reply *Reply
}

type Err string

// Put or Append
type Args struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SerialID int
}

func (args Args) GetClientID() int64 { return args.ClientID }

func (args Args) GetSerialID() int { return args.SerialID }

type Reply struct {
	Err   Err
	Value string
}

func (reply Reply) GetErr() Err { return reply.Err }

func FuncLatency(name string, start time.Time, args ...interface{}) {
	defer log.Infof("[Function Latency][%v] latency %.2fs. args: %+v", name, time.Since(start).Seconds(), args)
}

func isReplySuccess(err Err) bool {
	return err == OK || err == ""
}
