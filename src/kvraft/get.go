package kvraft

import (
	log "github.com/sirupsen/logrus"
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// defer FuncLatency("KVServer.RPC.Get", time.Now(), args, reply)

	// log.Infof("[RPC Get] Start! Server %v, reqID: %v, args: %+v, reply: %+v", kv.me, args.RequestID, args, reply)

	kv.requestCh <- Request{
		Args:  args,
		Reply: reply,
	}
	<-kv.requestDoneCh

	// if !isReplySuccess(reply.Err) {
	log.Infof("[RPC Get] Finished! Server %v, reqID: %v, args: %+v, reply: %+v", kv.me, args.RequestID, args, reply)
	// }
}

func (kv *KVServer) handleGetRequest(args *GetArgs, reply *GetReply) {
	defer func() { kv.requestDoneCh <- struct{}{} }()

	latestRequestID := kv.getLatestRequestID()
	if args.RequestID < latestRequestID {
		reply.Err = ErrOutOfDate
		return
	}

	index, _, isLeader := kv.rf.Start(Op{
		Name: "Get",
		Key:  args.Key,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.waitForIndexApplied(index)

	kv.mu.Lock()
	v, ok := kv.store[args.Key]
	kv.mu.Unlock()

	if ok {
		reply.Value = v
	} else {
		reply.Err = ErrNoKey
	}
	kv.updateLatestRequestID(args.RequestID, reply.Err)
}
