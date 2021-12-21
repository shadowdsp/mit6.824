package kvraft

import (
	log "github.com/sirupsen/logrus"
)

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// defer FuncLatency("KVServer.RPC.PutAppend", time.Now(), args, reply)

	// log.Infof("[RPC PutAppend] Start! Server %v, reqID: %v, args: %+v, reply: %+v", kv.me, args.RequestID, args, reply)

	kv.requestCh <- Request{
		Args:  args,
		Reply: reply,
	}
	<-kv.requestDoneCh

	// if !isReplySuccess(reply.Err) {
	log.Infof("[RPC PutAppend] Finished! Server %v, reqID: %v, args: %+v, reply: %+v", kv.me, args.RequestUid, args, reply)
	// }
}

func (kv *KVServer) handlePutAppendRequest(args *PutAppendArgs, reply *PutAppendReply) {
	defer func() { kv.requestDoneCh <- struct{}{} }()

	if kv.isRequestedUid(args.GetRequestUid()) {
		reply.Err = ErrOutOfDate
		return
	}

	index, _, isLeader := kv.rf.Start(Op{
		Name:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.waitForIndexApplied(index)
	kv.updateRequestUid(args.RequestUid, reply.Err)
}
