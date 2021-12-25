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
	RetryInterval   = 10 * time.Millisecond
	rpcTimeoutLimit = 50 * time.Millisecond
)

type Clerk struct {
	mu sync.Mutex // guards

	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderID int
	serialID     int
	clientID     int64
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
	ck.serialID = 0
	ck.clientID = nrand()
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

func (ck *Clerk) getAndIncSerialID() int {
	ck.serialID += 1
	return ck.serialID
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

func (ck *Clerk) sendRequestToServer(op string, key string, value string, serverID int) (string, bool) {
	success := true
	result := ""

	args := Args{
		Op:       op,
		Key:      key,
		Value:    value,
		ClientID: ck.clientID,
		SerialID: ck.serialID,
	}
	reply := Reply{}
	// ok := ck.servers[serverID].Call(RpcNameKVRequest, &args, &reply)
	err := RpcCallWithTimeout(ck.servers[serverID], RpcNameKVRequest, &args, &reply, rpcTimeoutLimit)

	if err != nil || reply.Err == ErrWrongLeader {
		success = false
	} else if reply.Err == OK {
		result = reply.Value
	}
	return result, success
}

func (ck *Clerk) sendRequest(op string, key string, value string) string {
	ck.mu.Lock()
	ck.getAndIncSerialID()
	log.Infof("[Clerk.sendRequest] ClientID %v, SerialID %v, key %v, value %v, op %v", ck.clientID, ck.serialID, key, value, op)
	ck.mu.Unlock()
	for {
		time.Sleep(RetryInterval)

		if result, success := ck.sendRequestToServer(op, key, value, ck.lastLeaderID); success {
			return result
		}

		for i := range ck.servers {
			if i == ck.lastLeaderID {
				continue
			}
			if result, success := ck.sendRequestToServer(op, key, value, i); success {
				ck.lastLeaderID = i
				return result
			}
		}
	}
}

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.sendRequest("Get", key, "")
}

func (ck *Clerk) Put(key string, value string) {
	ck.sendRequest("Put", key, value)
}

func (ck *Clerk) Append(key string, value string) {
	ck.sendRequest("Append", key, value)
}
