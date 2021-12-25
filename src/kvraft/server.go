package kvraft

import (
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	"mit6.824/src/labgob"
	"mit6.824/src/labrpc"
	"mit6.824/src/raft"

	log "github.com/sirupsen/logrus"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SerialID int
	// op name
	Name string
	// op key/value
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	requestCh     chan Request
	requestDoneCh chan struct{}

	clientMaxSerialID map[int64]int
	appliedOpCh       map[int]chan Op
	store             map[string]string
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) cleanUpIfKilled() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Infof("[cleanUpIfKilled] KV Server %v was killed", kv.me)
	kv.rf.Kill()
}

func (kv *KVServer) closeWaitCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer func() {
		if recover() != nil {
		}
	}()
	close(kv.appliedOpCh[index])
	kv.appliedOpCh[index] = nil
}

func (kv *KVServer) handleEvent() {
	for {
		if kv.killed() {
			kv.cleanUpIfKilled()
			return
		}

		select {
		case req := <-kv.requestCh:
			kv.handleKVRequest(req.Args, req.Reply)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// You may need initialization code here.
	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),

		requestCh:         make(chan Request),
		requestDoneCh:     make(chan struct{}),
		clientMaxSerialID: make(map[int64]int),
		appliedOpCh:       make(map[int]chan Op),

		store: make(map[string]string),
	}

	// You may need initialization code here.
	go kv.handleEvent()
	go kv.handleApplyCh()

	return kv
}

func startPprofServer() {
	http.ListenAndServe("0.0.0.0:6060", nil)
}

func init() {
	log.SetOutput(os.Stdout)
	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
	log.SetLevel(log.InfoLevel)
	log.SetLevel(log.WarnLevel)
	log.SetFormatter(&log.TextFormatter{
		// DisableColors: true,
		FullTimestamp: true,
	})
	go startPprofServer()
}
