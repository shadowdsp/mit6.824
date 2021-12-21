package kvraft

import (
	"crypto/rand"
	"fmt"
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
	uid          int64
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
	ck.uid = nrand()
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

func (ck *Clerk) getRequestUid() string {
	return fmt.Sprintf("%v-%v", ck.uid, ck.requestID)
}

func (ck *Clerk) getAndIncRequestID() int {
	ck.requestID += 1
	return ck.requestID
}

func (ck *Clerk) get(requestUid string, serverID int, key string) (string, bool) {
	value, success := "", true

	args := GetArgs{
		RequestUid: requestUid,
		Key:        key,
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
	ck.mu.Lock()
	ck.getAndIncRequestID()
	requestUid := ck.getRequestUid()
	log.Infof("[Clerk.Get] reqUID %v, key %v", requestUid, key)
	ck.mu.Unlock()
	for {
		time.Sleep(RetryInterval)

		if v, success := ck.get(requestUid, ck.lastLeaderID, key); success {
			return v
		}

		for i := range ck.servers {
			if i == ck.lastLeaderID {
				continue
			}
			if v, success := ck.get(requestUid, i, key); success {
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
func (ck *Clerk) putAppend(requestUid string, serverID int,
	key string, value string, op string) bool {

	success := true
	args := PutAppendArgs{
		RequestUid: requestUid,
		Key:        key,
		Value:      value,
		Op:         op,
	}
	reply := PutAppendReply{}
	ok := ck.servers[serverID].Call(RpcNamePutAppend, &args, &reply)
	if !ok || reply.Err == ErrWrongLeader {
		success = false
	}
	return success
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.getAndIncRequestID()
	requestUid := ck.getRequestUid()
	log.Infof("[Clerk.PutAppend] reqUID %v, key %v, value %v, op %v", requestUid, key, value, op)
	ck.mu.Unlock()
	for {
		time.Sleep(RetryInterval)

		if success := ck.putAppend(requestUid, ck.lastLeaderID, key, value, op); success {
			return
		}

		for i := range ck.servers {
			if i == ck.lastLeaderID {
				continue
			}
			if success := ck.putAppend(requestUid, i, key, value, op); success {
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
