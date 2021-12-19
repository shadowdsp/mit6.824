package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"mit6.824/src/labrpc"
)

const (
	RetryInterval = 10 * time.Millisecond
)

type Clerk struct {
	mu sync.Mutex // guards

	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderID int
	requestID    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeaderID = 0
	ck.requestID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) getAndIncRequestID() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestID++
	return ck.requestID
}

func (ck *Clerk) get(requestID int, serverID int, key string) (string, bool) {
	value, success := "", true

	args := GetArgs{
		RequestID: requestID,
		Key:       key,
	}
	reply := GetReply{}
	ok := ck.servers[serverID].Call(RpcNameGet, &args, &reply)
	if !ok || reply.Err == ErrWrongLeader {
		success = false
	}
	if ok {
		value = reply.Value
	}
	return value, success
}

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	requestID := ck.getAndIncRequestID()
	// defer FuncLatency("Clerk.Get", time.Now(), requestID)
	log.Infof("[Clerk.Get] reqID %v, key %v", requestID, key)
	for {
		time.Sleep(RetryInterval)

		if v, success := ck.get(requestID, ck.lastLeaderID, key); success {
			return v
		}

		for i := range ck.servers {
			if i == ck.lastLeaderID {
				continue
			}
			if v, success := ck.get(requestID, i, key); success {
				ck.lastLeaderID = i
				return v
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) putAppend(requestID int, serverID int,
	key string, value string, op string) bool {

	success := true
	args := PutAppendArgs{
		RequestID: requestID,
		Key:       key,
		Value:     value,
		Op:        op,
	}
	reply := PutAppendReply{}
	ok := ck.servers[serverID].Call(RpcNamePutAppend, &args, &reply)
	if !ok || reply.Err == ErrWrongLeader {
		success = false
	}
	return success
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestID := ck.getAndIncRequestID()

	// defer FuncLatency("Clerk.PutAppend", time.Now(), requestID)
	log.Infof("[Clerk.PutAppend] reqID %v, key %v, value %v, op %v", requestID, key, value, op)
	// You will have to modify this function.
	for {
		time.Sleep(RetryInterval)

		if success := ck.putAppend(requestID, ck.lastLeaderID, key, value, op); success {
			return
		}

		for i := range ck.servers {
			if i == ck.lastLeaderID {
				continue
			}
			if success := ck.putAppend(requestID, i, key, value, op); success {
				ck.lastLeaderID = i
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
