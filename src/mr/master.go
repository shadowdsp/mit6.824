package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	nReduce            int
	safeMapTaskInfo    *SafeMapTaskInfo
	safeReduceTaskInfo *SafeReduceTaskInfo
	completed          bool
}

// MapTaskStatus MapTaskStatus
type MapTaskStatus struct {
	// TODO: use enum
	// 0 unallocated, 1 allocated and incompleted, 2 completed
	Status                uint8
	StartTime             time.Time
	CompleteTime          time.Time
	MapTaskID             string
	IntermediateFilepaths []string
}

func (st *MapTaskStatus) isCompleted() bool {
	return st.Status == TaskCompleted
}

func (st *MapTaskStatus) isPending() bool {
	return st.Status == TaskPending
}

func (st *MapTaskStatus) isTimeout() bool {
	return !st.isCompleted() && st.StartTime.Add(TimeoutLimit).Before(time.Now())
}

// SafeMapTaskInfo SafeMapTaskInfo
type SafeMapTaskInfo struct {
	tasks     map[string]*MapTaskStatus
	filepaths []string
	mux       sync.Mutex
}

func newMapTaskInfo(filepaths []string) *SafeMapTaskInfo {
	mapTaskInfo := SafeMapTaskInfo{}
	mapTaskInfo.filepaths = filepaths
	mapTaskInfo.tasks = make(map[string]*MapTaskStatus)
	return &mapTaskInfo
}

func newMapTaskStatus() *MapTaskStatus {
	return &MapTaskStatus{
		Status: TaskPending,
	}
}

func (st *ReduceTaskStatus) isCompleted() bool {
	return st.Status == TaskCompleted
}

func (st *ReduceTaskStatus) isPending() bool {
	return st.Status == TaskPending
}

func (st *ReduceTaskStatus) isTimeout() bool {
	return !st.isCompleted() && st.StartTime.Add(TimeoutLimit).Before(time.Now())
}

// ReduceTaskStatus ReduceTaskStatus
type ReduceTaskStatus struct {
	// TODO: use enum
	// 0 unallocated, 1 allocated and incompleted, 2 completed
	Status           uint8
	StartTime        time.Time
	CompleteTime     time.Time
	ReduceTaskID     string
	ReduceInputPaths []string
	ReduceOutputPath string
}

// SafeReduceTaskInfo SafeReduceTaskInfo
type SafeReduceTaskInfo struct {
	tasks               map[string]*ReduceTaskStatus
	initiliazeCompleted bool
	mux                 sync.Mutex
}

func newReduceTaskStatus() *ReduceTaskStatus {
	return &ReduceTaskStatus{
		Status: TaskPending,
	}
}

func (m *Master) initializeReduceTask() error {
	m.safeReduceTaskInfo.tasks = make(map[string]*ReduceTaskStatus, m.nReduce)
	for i := 0; i < m.nReduce; i++ {
		m.safeReduceTaskInfo.tasks[strconv.Itoa(i)] = newReduceTaskStatus()
		status := m.safeReduceTaskInfo.tasks[strconv.Itoa(i)]
		status.ReduceTaskID = strconv.Itoa(i)
	}

	m.safeMapTaskInfo.mux.Lock()
	defer m.safeMapTaskInfo.mux.Unlock()
	for f, status := range m.safeMapTaskInfo.tasks {
		log.Debugf("[Master.initializeReduceTask] Map file %v, status %v, len(ifile) %v, ifile %+v", f, status.Status, len(status.IntermediateFilepaths), status.IntermediateFilepaths)
		for _, filepath := range status.IntermediateFilepaths {
			tmp := strings.Split(filepath, "-")
			reduceID := tmp[len(tmp)-1]
			if rStatus, ok := m.safeReduceTaskInfo.tasks[reduceID]; ok {
				rStatus.ReduceInputPaths = append(rStatus.ReduceInputPaths, filepath)
				// log.Debugf("[Master.initializeReduceTask] reduceTaskID %v has input paths: %+v", rStatus.ReduceTaskID, rStatus.ReduceInputPaths)
			} else {
				err := fmt.Errorf("IntermediateFilepath %v is not match reduce number %v", filepath, m.nReduce)
				log.Errorf(err.Error())
				return err
			}
		}
	}
	m.safeReduceTaskInfo.initiliazeCompleted = true
	return nil
}

// TaskAllocations map[filename]MapTaskID
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
	// TODO: change to filename hash
	resp.MapTaskID = uuid.New().String()[:8]
	resp.AllCompleted = true
	filePaths := []string{}
	// Traverse task and get the incompleted task, need to lock
	// tips: if startTime + 10 < nowTime, this task should be redo
	m.safeMapTaskInfo.mux.Lock()
	defer m.safeMapTaskInfo.mux.Unlock()
	// TODO: if all map tasks are allocated, should notify worker to do reduce
	log.Debugf("Master.GetMapTask filepaths: %+v\n", m.safeMapTaskInfo.filepaths)
	for _, filepath := range m.safeMapTaskInfo.filepaths {
		if m.safeMapTaskInfo.tasks[filepath] == nil {
			m.safeMapTaskInfo.tasks[filepath] = newMapTaskStatus()
		}
		status := m.safeMapTaskInfo.tasks[filepath]
		if status.isPending() || status.isTimeout() {
			// can be allocated
			filePaths = append(filePaths, filepath)
			// mark filepath is allocated
			status.StartTime = time.Now()
			status.Status = TaskRunning
			status.MapTaskID = resp.MapTaskID
			resp.AllCompleted = false
			// one task one filepath
			break
		}
		log.Debugf("Master.GetMapTask status: %+v\n", status)
	}
	resp.Filepaths = filePaths
	log.Debugf("Master.GetMapTask resp: %+v", resp)
	return nil
}

// CompleteMapTask CompleteMapTask
func (m *Master) CompleteMapTask(req *CompleteMapTaskRequest, resp *CompleteMapTaskResponse) error {
	m.safeMapTaskInfo.mux.Lock()
	defer m.safeMapTaskInfo.mux.Unlock()
	for _, filepath := range req.Filepaths {
		status, ok := m.safeMapTaskInfo.tasks[filepath]
		if !ok {
			err := fmt.Errorf("file path %+v is not exist in task info", filepath)
			log.Errorf(err.Error())
			return err
		}
		if status.MapTaskID != req.MapTaskID {
			err := fmt.Errorf("Master map id %v is not match request map id %v", status.MapTaskID, req.MapTaskID)
			log.Warnf(err.Error())
			return err
		}
		if status.isCompleted() {
			err := fmt.Errorf("Master map job is completed, id: %v", status.MapTaskID)
			log.Warnf(err.Error())
			return err
		}
		log.Infof("[Master.CompleteMapTask] Map file %v, id %v succeeded", filepath, req.MapTaskID)
		status.Status = TaskCompleted
		status.CompleteTime = time.Now()
		status.IntermediateFilepaths = req.IntermediateFilepaths
	}
	return nil
}

// GetReduceTask GetReduceTask
func (m *Master) GetReduceTask(req *GetReduceTaskRequest, resp *GetReduceTaskResponse) error {
	m.safeReduceTaskInfo.mux.Lock()
	defer m.safeReduceTaskInfo.mux.Unlock()

	if !m.safeReduceTaskInfo.initiliazeCompleted {
		// initialize reduce tasks
		err := m.initializeReduceTask()
		if err != nil {
			return err
		}
	}
	resp.AllCompleted = true
	for _, status := range m.safeReduceTaskInfo.tasks {
		if status.isPending() || status.isTimeout() {
			resp.Filepaths = status.ReduceInputPaths
			resp.ReduceTaskID = status.ReduceTaskID
			status.Status = TaskRunning
			status.StartTime = time.Now()
			resp.AllCompleted = false
			break
		}
	}
	if resp.AllCompleted {
		m.completed = true
	}
	log.Debugf("Master.GetReduceTask resp: %+v", resp)
	return nil
}

// CompleteReduceTask CompleteReduceTask
func (m *Master) CompleteReduceTask(req *CompleteReduceTaskRequest, resp *CompleteReduceTaskResponse) error {
	m.safeReduceTaskInfo.mux.Lock()
	defer m.safeReduceTaskInfo.mux.Unlock()
	status, ok := m.safeReduceTaskInfo.tasks[req.ReduceTaskID]
	if !ok {
		err := fmt.Errorf("Execute CompleteReduceTask() failed, ReduceTaskID %v cannot be found in tasks", req.ReduceTaskID)
		log.Errorf(err.Error())
		return err
	}
	status.Status = TaskCompleted
	status.CompleteTime = time.Now()
	status.ReduceOutputPath = req.ReduceOutputPath
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	// if m.completed {
	// 	ret = true
	// }
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.Infof("Master nReduce: %+v\n", nReduce)
	log.Infof("Master resolve files: %+v\n", files)
	m := Master{
		nReduce:         nReduce,
		safeMapTaskInfo: newMapTaskInfo(files),
		safeReduceTaskInfo: &SafeReduceTaskInfo{
			initiliazeCompleted: false,
		},
	}

	// Your code here.

	m.server()
	return &m
}
