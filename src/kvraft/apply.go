package kvraft

import (
	"time"

	log "github.com/sirupsen/logrus"
)

func (kv *KVServer) handleApplyCh() {
	for msg := range kv.applyCh {
		if kv.killed() {
			kv.cleanUpIfKilled()
			return
		}

		if !msg.CommandValid {
			continue
		}
		kv.applyOp(msg.CommandIndex, msg.Command.(Op))
	}
}

func (kv *KVServer) applyOp(index int, op Op) {
	time.Sleep(10 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	log.Infof("[applyOp] Server %v applied in index %v, op %+v", kv.me, index, op)

	if serialID, ok := kv.clientMaxSerialID[op.ClientID]; !ok || serialID < op.SerialID {
		switch op.Name {
		// case OpNameGet:
		case OpNamePut:
			kv.store[op.Key] = op.Value
		case OpNameAppend:
			if _, ok := kv.store[op.Key]; !ok {
				kv.store[op.Key] = ""
			}
			kv.store[op.Key] += op.Value
		}
		kv.clientMaxSerialID[op.ClientID] = op.SerialID
	}

	if waitCh, ok := kv.appliedOpCh[index]; ok {
		log.Infof("[applyOp] Server %v is sending op to appliedOpCh, index %+v",
			kv.me, index)
		waitCh <- op
	} else {
		log.Infof("[applyOp] Server %v failed to find index %v in appliedOpCh, op %+v",
			kv.me, index, op)
	}
}
