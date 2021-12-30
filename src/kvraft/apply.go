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
			kv.applySnapshot(msg.Data, msg.LastIncludedIndex)
		} else {
			kv.applyOp(msg.CommandIndex, msg.Command.(Op))
		}
	}
}

func (kv *KVServer) applyOp(index int, op Op) {
	time.Sleep(10 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	log.Infof("[applyOp] KVServer %v applying index %v, op %+v", kv.me, index, op)

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
		kv.lastAppliedIndex = index
		log.Infof("[applyOp] KVServer %v update store successfully, index %v, SerialID %v",
			kv.me, index, op.SerialID)
	}

	if waitCh, ok := kv.appliedOpCh[index]; ok {
		log.Infof("[applyOp] KVServer %v is sending op to appliedOpCh, index %+v, lastAppliedIndex: %v",
			kv.me, index, kv.lastAppliedIndex)
		waitCh <- op
	} else {
		// because of log replication
		log.Infof("[applyOp] KVServer %v failed to find index %v in appliedOpCh, lastAppliedIndex: %v, op %+v",
			kv.me, index, kv.lastAppliedIndex, op)
	}
}

func (kv *KVServer) applySnapshot(data []byte, lastIncludedIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.store, kv.clientMaxSerialID = kv.decodeSnapshot(data)
	kv.lastAppliedIndex = lastIncludedIndex
	log.Infof("[applySnapshot] KVServer %v applied snapshot, index: %v, store %+v, clientMaxSerialID %+v",
		kv.me, lastIncludedIndex, kv.store, kv.clientMaxSerialID)
}
