package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	TaskPending   = "Pending"
	TaskRunning   = "Running"
	TaskCompleted = "Completed"

	TaskTypeMap    = "Map"
	TaskTypeReduce = "Reduce"
	TaskTypeNone   = "None"

	TaskTimeoutLimit = 10 * time.Second
)

// Master master server
type Master struct {
	// Your definitions here.
	nReduce                    int
	mapTasks                   []*Task
	reduceTasks                []*Task
	incompletedMapTaskCount    int
	incompletedReduceTaskCount int
	reduceInitialized          bool
	mux                        sync.Mutex
}

func (t *Task) isCompleted() bool {
	return t.phase == TaskCompleted
}

func (t *Task) isPending() bool {
	return t.phase == TaskPending
}

type Task struct {
	// Pending, Running and Completed
	phase    string
	taskID   int
	taskType string
	// for map, input path has only one element
	inputPaths []string
	// for reduce, output path has only one element
	outputPaths []string
}

func (m *Master) initReduceTasksInput() error {
	reduceTasksInput := make([][]string, m.nReduce)
	for _, t := range m.mapTasks {
		for _, out := range t.outputPaths {
			tmp := strings.Split(out, "-")
			reduceID, err := strconv.Atoi(tmp[len(tmp)-1])
			if err != nil {
				log.Errorf("[initReduceTasksInput] parse filename err: %v", err)
				return err
			}
			reduceTasksInput[reduceID] = append(reduceTasksInput[reduceID], out)
		}
	}

	for reduceID, task := range m.reduceTasks {
		task.inputPaths = reduceTasksInput[reduceID]
	}
	return nil
}

func (m *Master) checkTaskTimeout(task *Task) {
	<-time.After(TaskTimeoutLimit)
	m.mux.Lock()
	defer m.mux.Unlock()
	if task.phase == TaskCompleted {
		return
	}
	switch task.taskType {
	case TaskTypeMap:
		m.mapTasks[task.taskID].phase = TaskPending
	case TaskTypeReduce:
		m.reduceTasks[task.taskID].phase = TaskPending
	default:
		log.Errorf("unknown task type %v in checkTaskTimeout", task.taskType)
	}
}

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

func (m *Master) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	resp.NReduce = m.nReduce
	var tasks []*Task
	if m.incompletedMapTaskCount > 0 {
		tasks = m.mapTasks
	} else if m.incompletedReduceTaskCount > 0 {
		tasks = m.reduceTasks
	} else {
		resp.TaskType = TaskTypeNone
		return nil
	}

	// task pending/running
	for _, t := range tasks {
		if t.isPending() {
			resp.TaskID = t.taskID
			resp.TaskInputs = t.inputPaths
			resp.TaskType = t.taskType
			log.Debugf("[Master.GetTask] pending task: %+v", t)
			t.phase = TaskRunning
			go m.checkTaskTimeout(t)
			break
		}
	}

	if len(resp.TaskInputs) <= 0 {
		resp.TaskType = TaskTypeNone
	}
	return nil
}

func (m *Master) CompleteTask(req *CompleteTaskRequest, resp *CompleteTaskResponse) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	if req.TaskType == TaskTypeMap {
		m.mapTasks[req.TaskID].phase = TaskCompleted
		m.mapTasks[req.TaskID].outputPaths = req.TaskOutputs
		m.incompletedMapTaskCount--
	} else if req.TaskType == TaskTypeReduce {
		m.reduceTasks[req.TaskID].phase = TaskCompleted
		m.reduceTasks[req.TaskID].outputPaths = req.TaskOutputs
		m.incompletedReduceTaskCount--
	}

	if m.incompletedMapTaskCount <= 0 && !m.reduceInitialized {
		err := m.initReduceTasksInput()
		if err != nil {
			os.Exit(1)
		}
		m.reduceInitialized = true
	}
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
	if m.incompletedMapTaskCount <= 0 && m.incompletedReduceTaskCount <= 0 {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	log.Infof("Master nReduce: %+v\n", nReduce)
	log.Infof("Master resolve files: %+v\n", files)
	mapTasks := []*Task{}
	for i, file := range files {
		mapTasks = append(mapTasks, &Task{
			phase:      TaskPending,
			taskID:     i,
			taskType:   TaskTypeMap,
			inputPaths: []string{file},
		})
	}
	reduceTasks := []*Task{}
	for i := 0; i < nReduce; i++ {
		reduceTasks = append(reduceTasks, &Task{
			phase:    TaskPending,
			taskID:   i,
			taskType: TaskTypeReduce,
		})
	}
	m := Master{
		nReduce:                    nReduce,
		mapTasks:                   mapTasks,
		reduceTasks:                reduceTasks,
		incompletedMapTaskCount:    len(mapTasks),
		incompletedReduceTaskCount: len(reduceTasks),
	}
	m.server()
	return &m
}
