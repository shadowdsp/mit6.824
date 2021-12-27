package kvraft

import (
	"bytes"
	"time"

	"mit6.824/src/labgob"
)

func (kv *KVServer) isNeedSnapshot() bool {
	return kv.maxraftstate != -1 && kv.maxraftstate < kv.rf.GetStateSize()
}

func (kv *KVServer) encodeSnapshot() ([]byte, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// snapshot store and request serialID
	e.Encode(kv.store)
	e.Encode(kv.clientMaxSerialID)
	return w.Bytes(), kv.lastAppliedIndex
}

func (kv *KVServer) decodeSnapshot(data []byte) (map[string]string, map[int64]int) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return make(map[string]string), make(map[int64]int)
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var store map[string]string
	var clientMaxSerialID map[int64]int
	d.Decode(&store)
	d.Decode(&clientMaxSerialID)
	return store, clientMaxSerialID
}

func (kv *KVServer) handleSnapshot() {
	for {
		time.Sleep(time.Millisecond * 50)

		if kv.isNeedSnapshot() {
			kv.rf.DoSnapshot(kv.encodeSnapshot())
		}
	}
}
