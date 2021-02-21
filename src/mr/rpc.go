package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// GetMapTaskRequest GetMapTaskRequest
type GetMapTaskRequest struct{}

// GetMapTaskResponse GetMapTaskResponse
type GetMapTaskResponse struct {
	Filepaths    []string
	MapTaskID    string
	NReduce      int
	AllCompleted bool
}

type CompleteMapTaskRequest struct {
	Filepaths             []string
	MapTaskID                 string
	IntermediateFilepaths []string
}

type CompleteMapTaskResponse struct{}

type GetReduceTaskRequest struct{}

type GetReduceTaskResponse struct {
	Filepaths []string
	ReduceTaskID string
	AllCompleted bool
}

type CompleteReduceTaskRequest struct {
	Filepaths []string
	ReduceTaskID string
	ReduceOutputPath string
}

type CompleteReduceTaskResponse struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
