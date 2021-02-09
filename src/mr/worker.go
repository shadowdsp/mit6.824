package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"

	"net/rpc"
	"os"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func saveKV(kva []KeyValue, file *os.File) error {
	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

// func loadKV(kva []KeyValue, file *os.File) error {
// 	dec := json.NewDecoder(file)
// 	for {
// 		var kv KeyValue
// 		if err := dec.Decode(&kv); err != nil {
// 			return err
// 		}
// 		kva = append(kva, kv)
// 	}
// 	return nil
// }

func resolveMapTask(
	mapID string,
	filenames []string,
	nReduce int,
	mapf func(string, string) []KeyValue,
) ([]string, error) {
	// execute map task
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Errorf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Errorf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// sort the intermediate value
	sort.Sort(ByKey(intermediate))
	intermediateFilenames := []string{}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		tmpKvs := []KeyValue{}
		for k := i; k < j; k++ {
			tmpKvs = append(tmpKvs, intermediate[k])
		}
		// output := reducef(intermediate[i].Key, values)
		reduceID := ihash(intermediate[i].Key) % nReduce
		intermediateFilename := fmt.Sprintf("mr-%v-%v", mapID, reduceID)
		ofile, _ := os.Create(intermediateFilename)
		err := saveKV(tmpKvs, ofile)
		if err != nil {
			return nil, err
		}
		intermediateFilenames = append(intermediateFilenames, intermediateFilename)
		i = j
	}
	return intermediateFilenames, nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// Map part
	taskQueryDuration := time.Duration(time.Millisecond * 500)
	for range time.Tick(taskQueryDuration) {
		resp, err := rpcGetMapTask()
		if err != nil {
			log.Errorf("Failed to execute rpcGetMapTask(): %v", err)
			return
		}
		if resp.AllCompleted {
			// all map task are completed
			break
		}
		// concurrently resolve map task
		go func(resp *GetMapTaskResponse) {
			intermediateFilenames, err := resolveMapTask(resp.MapTaskID, resp.Filepaths, resp.NReduce, mapf)
			if err != nil {
				log.Errorf("Failed to execute resolveMapTask(): %v", err)
				return
			}

			completeMapTaskRequest := CompleteMapTaskRequest{
				Filepaths:             resp.Filepaths,
				MapID:                 resp.MapTaskID,
				IntermediateFilepaths: intermediateFilenames,
			}
			ok := call("Master.CompleteMapTask", &completeMapTaskRequest, &CompleteMapTaskResponse{})
			if !ok {
				log.Errorf("RPC CompleteMapTask failed")
				return
			}
		}(resp)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// RPCGetMapTask call for map task
func rpcGetMapTask() (*GetMapTaskResponse, error) {
	response := GetMapTaskResponse{}
	ok := call("Master.GetMapTask", &GetMapTaskRequest{}, &response)
	if !ok {
		return nil, errors.New("RPCGetMapTask failed")
	}
	fmt.Printf("RPCGetMapTask result: %+v\n", response)
	return &response, nil
}

func rpcCompleteMapTask() (*CompleteMapTaskResponse, error) {
	response := CompleteMapTaskResponse{}
	ok := call("Master.CompleteMapTask", &CompleteMapTaskRequest{}, &response)
	if !ok {
		return nil, errors.New("RPCCompleteMapTask failed")
	}
	fmt.Printf("RPCCompleteMapTask result: %+v\n", response)
	return &response, nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }
