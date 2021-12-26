package kvraft

import (
	"time"

	log "github.com/sirupsen/logrus"
	"mit6.824/src/raft"
)

func (kv *KVServer) handleApplyCh() {
	for msg := range kv.applyCh {
		if kv.killed() {
			kv.cleanUpIfKilled()
			return
		}

		kv.applyOp(msg)
	}
}

func (kv *KVServer) applyOp(msg raft.ApplyMsg) {
	time.Sleep(10 * time.Millisecond)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	term, index, op := msg.CommandTerm, msg.CommandIndex, msg.Command.(Op)
	log.Infof("[applyOp] Server %v applied in index %v, op %+v", kv.me, index, op)

	if msg.CommandValid {
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
			kv.lastIncludedIndex = index
			kv.lastIncludedTerm = term
		}
	}

	if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
		snapshot := raft.Snapshot{
			LastIncludedIndex: kv.lastIncludedIndex,
			LastIncludedTerm:  kv.lastIncludedTerm,
			Store:             kv.store,
		}
		// kv.saveSnapshot()
		kv.rf.DoSnapshot(snapshot)
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
