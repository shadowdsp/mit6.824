package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	TaskPending   = 0
	TaskRunning   = 1
	TaskCompleted = 2
	TimeoutLimit  = 10 * time.Second
)

type Master struct {
	// Your definitions here.
	nReduce         int
	safeMapTaskInfo SafeMapTaskInfo
}

// MapTaskStatus MapTaskStatus
type MapTaskStatus struct {
	// TODO: use enum
	// 0 unallocated, 1 allocated and incompleted, 2 completed
	Status       uint8
	StartTime    time.Time
	CompleteTime time.Time
	MapTaskID    string
}

func (st *MapTaskStatus) isCompleted() bool {
	return st.Status == TaskCompleted
}

func (st *MapTaskStatus) isPending() bool {
	return st.Status == TaskPending
}

func (st *MapTaskStatus) isTimeout() bool {
	return st.StartTime.Add(TimeoutLimit).Before(time.Now())
}

// SafeMapTaskInfo SafeMapTaskInfo
type SafeMapTaskInfo struct {
	tasks map[string]*MapTaskStatus
	mux   sync.Mutex
}

// TaskAllocations map[filename]mapID
// type TaskAllocations map[string][]string

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetMapTask GetMapTask
func (m *Master) GetMapTask(req *GetMapTaskRequest, resp *GetMapTaskResponse) error {
	// TODO: get uuid here
	resp.NReduce = m.nReduce
	resp.MapTaskID = uuid.New().String()
	resp.AllCompleted = true
	filePaths := make([]string, 1)
	// Traverse task and get the incompleted task, need to lock
	// tips: if startTime + 10 < nowTime, this task should be redo
	m.safeMapTaskInfo.mux.Lock()
	defer m.safeMapTaskInfo.mux.Unlock()
	// TODO: if all map tasks are allocated, should notify worker to do reduce
	for filepath, status := range m.safeMapTaskInfo.tasks {
		if status.isPending() || status.isTimeout() {
			// can be allocated
			filePaths = append(filePaths, filepath)
			// mark filepath is allocated
			m.safeMapTaskInfo.tasks[filepath].StartTime = time.Now()
			m.safeMapTaskInfo.tasks[filepath].Status = TaskRunning
			m.safeMapTaskInfo.tasks[filepath].MapTaskID = resp.MapTaskID
			// m.taskAllocations[filepath] = append(m.taskAllocations[filepath], resp.MapID)
			// one task one filepath
			break
		}
		if !status.isCompleted() {
			resp.AllCompleted = false
		}
	}
	resp.Filepaths = filePaths
	return nil
}

// CompleteMapTask CompleteMapTask
func (m *Master) CompleteMapTask(req *CompleteMapTaskRequest, resp *CompleteMapTaskResponse) error {
	m.safeMapTaskInfo.mux.Lock()
	defer m.safeMapTaskInfo.mux.Unlock()
	for _, filepath := range req.Filepaths {
		// add lock
		if m.safeMapTaskInfo.tasks[filepath] == nil {
			err := fmt.Errorf("file path %+v is not exist in task info", filepath)
			log.Errorf(err.Error())
			return err
		}
		if m.safeMapTaskInfo.tasks[filepath].MapTaskID != req.MapID {
			err := fmt.Errorf("Master map id %v is not match request map id %v")
			log.Warnf(err.Error())
			return err
		}
		log.Infof("Map id %v succeeded", req.MapID)
		m.safeMapTaskInfo.tasks[filepath].Status = TaskCompleted
		m.safeMapTaskInfo.tasks[filepath].CompleteTime = time.Now()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
