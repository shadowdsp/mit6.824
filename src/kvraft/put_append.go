package kvraft

import (
	log "github.com/sirupsen/logrus"
)

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// defer FuncLatency("KVServer.RPC.PutAppend", time.Now(), args, reply)

	log.Infof("[RPC PutAppend] Start! Server %v, reqID: %v, args: %+v, reply: %+v", kv.me, args.RequestID, args, reply)

	kv.requestCh <- Request{
		Args:  args,
		Reply: reply,
	}
	<-kv.requestDoneCh
	log.Infof("[RPC PutAppend] Finished! Server %v, reqID: %v, args: %+v, reply: %+v", kv.me, args.RequestID, args, reply)
}

func (kv *KVServer) handlePutAppendRequest(args *PutAppendArgs, reply *PutAppendReply) {
	index, _, isLeader := kv.rf.Start(Op{
		Name:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.requestDoneCh <- struct{}{}
		return
	}

	kv.waitForIndexApplied(index)
	kv.requestDoneCh <- struct{}{}
}
