package kvraft

import (
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	applyOpTimeoutLimit = time.Millisecond * 10000
)

func (kv *KVServer) KVRequest(args *Args, reply *Reply) {
	// Your code here.
	// defer FuncLatency("KVServer.RPC.PutAppend", time.Now(), args, reply)

	log.Infof("[KVRequest %v] Start! Server %v, ClientID: %v, SerialID: %v, args: %+v",
		args.Op, kv.me, args.ClientID, args.SerialID, args)

	kv.requestCh <- Request{
		Args:  args,
		Reply: reply,
	}
	<-kv.requestDoneCh

	// if !isReplySuccess(reply.Err) {
	log.Infof("[KVRequest %v] Finished! Server %v, ClientID: %v, SerialID: %v, args: %+v, reply: %+v",
		args.Op, kv.me, args.ClientID, args.SerialID, args, reply)
	// }
}

func (kv *KVServer) handleKVRequest(args *Args, reply *Reply) {
	defer func() { kv.requestDoneCh <- struct{}{} }()
	reply.Err = OK

	kv.mu.Lock()
	log.Infof("[handleKVRequest] KVServer %v handling request, args %+v", kv.me, args)
	if serialID, ok := kv.clientMaxSerialID[args.ClientID]; ok && serialID >= args.SerialID {
		if args.Op == OpNameGet {
			if v, exist := kv.store[args.Key]; exist {
				reply.Value = v
			} else {
				reply.Err = ErrNoKey
			}
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClientID: args.GetClientID(),
		SerialID: args.GetSerialID(),
		Name:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	log.Infof("[handleKVRequest] KVServer %v start to commit op %+v, index %v, isLeader %v", kv.me, op, index, isLeader)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	waitCh := make(chan Op)
	kv.appliedOpCh[index] = waitCh
	kv.mu.Unlock()

	log.Infof("[handleKVRequest] KVServer %v wait for applied op, index %+v, op %+v", kv.me, index, op)
	select {
	case appliedOp := <-waitCh:
		log.Infof("[handleKVRequest] KVServer %v resolved index %v op %+v", kv.me, index, op)
		if appliedOp.ClientID != args.ClientID || appliedOp.SerialID != args.SerialID {
			reply.Err = ErrWrongLeader
			return
		}
		kv.closeWaitCh(index)
		break
	case <-time.After(applyOpTimeoutLimit):
		log.Infof("[handleKVRequest] KVServer %v apply OP index %v timeout", kv.me, index)
		reply.Err = ErrWrongLeader
		if kv.killed() {
			kv.cleanUpIfKilled()
			return
		}
		kv.closeWaitCh(index)
		return
	}

	kv.mu.Lock()
	if v, exist := kv.store[args.Key]; exist {
		reply.Value = v
	} else {
		reply.Err = ErrNoKey
	}
	log.Infof("[handleKVRequest] KVServer %v applied op successfully, index %+v, reply %+v", kv.me, index, reply)
	kv.mu.Unlock()
}
