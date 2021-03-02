package mr

import (
	"encoding/json"
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
	Key   string `json:"key"`
	Value string `json:"value"`
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
			return nil, err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Errorf("cannot read %v", filename)
			return nil, err
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// sort the intermediate value
	sort.Sort(ByKey(intermediate))
	// log.Debugf("[Worker.resolveMapTask] filepath: %+v, intermediate: %+v", filenames, intermediate)
	intermediateFilenames := []string{}
	intermediateMap := make(map[string]struct{})
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

		// append mode
		if _, err := os.Stat(intermediateFilename); os.IsNotExist(err) {
			_, err = os.Create(intermediateFilename)
			if err != nil {
				return nil, err
			}
		}
		ofile, err := os.OpenFile(intermediateFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}

		err = saveKV(tmpKvs, ofile)
		if err != nil {
			return nil, err
		}
		ofile.Close()

		if _, ok := intermediateMap[intermediateFilename]; !ok {
			// Guarantee no duplication
			intermediateMap[intermediateFilename] = struct{}{}
			intermediateFilenames = append(intermediateFilenames, intermediateFilename)
		}
		i = j
	}
	return intermediateFilenames, nil
}

func loadKV(kvs *[]KeyValue, file *os.File) error {
	dec := json.NewDecoder(file)
	for dec.More() {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			return err
		}
		*kvs = append(*kvs, kv)
	}
	return nil
}

func resolveReduceTask(
	reduceTaskID string,
	filepaths []string,
	reducef func(string, []string) string,
) (string, error) {
	kvs := []KeyValue{}
	for _, filepath := range filepaths {
		ifile, err := os.Open(filepath)
		if err != nil {
			log.Errorf("cannot open filepath %v, err: %v", filepath, err)
			return "", err
		}
		err = loadKV(&kvs, ifile)
		if err != nil {
			return "", err
		}
	}
	log.Infof("[Worker.resolveReduceTask] reduceTaskID: %+v, filepath: %+v", reduceTaskID, filepaths)
	sort.Sort(ByKey(kvs))

	reduceOutputFilepath := "mr-out-" + reduceTaskID
	ofile, err := os.Create(reduceOutputFilepath)
	if err != nil {
		return "", err
	}
	defer ofile.Close()
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	return reduceOutputFilepath, nil
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
		// go func(resp *GetMapTaskResponse) {
		if resp.Filepaths == nil {
			continue
		}
		log.Debugf("[Worker] GetMapTaskResponse.Filepaths: %+v", resp.Filepaths)
		intermediateFilenames, err := resolveMapTask(resp.MapTaskID, resp.Filepaths, resp.NReduce, mapf)
		if err != nil {
			log.Errorf("Failed to execute resolveMapTask(): %v", err)
			continue
		}

		completeMapTaskRequest := CompleteMapTaskRequest{
			Filepaths:             resp.Filepaths,
			MapTaskID:             resp.MapTaskID,
			IntermediateFilepaths: intermediateFilenames,
		}
		err = call("Master.CompleteMapTask", &completeMapTaskRequest, &CompleteMapTaskResponse{})
		if err != nil {
			log.Errorf("Excute RPC CompleteMapTask failed, err: %v", err)
			continue
		}
		// }(resp)
	}

	log.Info("================================")
	log.Info("====== Map task succeeded ======")
	log.Info("================================")

	for range time.Tick(taskQueryDuration) {
		resp, err := rpcGetReduceTask()
		if err != nil {
			log.Errorf("Failed to execute rpcGetReduceTask(): %v", err)
			return
		}
		if resp.AllCompleted {
			break
		}
		// go func(resp *GetReduceTaskResponse) {
		if resp.Filepaths == nil {
			continue
		}
		reduceOutputFilepath, err := resolveReduceTask(resp.ReduceTaskID, resp.Filepaths, reducef)
		if err != nil {
			log.Errorf("Failed to execute resolveReduceTask(): %v", err)
			continue
		}

		completeReduceTaskRequest := CompleteReduceTaskRequest{
			ReduceTaskID:     resp.ReduceTaskID,
			ReduceOutputPath: reduceOutputFilepath,
		}
		err = call("Master.CompleteReduceTask", &completeReduceTaskRequest, &CompleteMapTaskResponse{})
		if err != nil {
			log.Errorf("Excute RPC CompleteReduceTask failed, err: %v", err)
			continue
		}
		// }(resp)
	}

	log.Info("================================")
	log.Info("===== Reduce task succeeded ====")
	log.Info("================================")
}

// RPCGetMapTask call for map task
func rpcGetMapTask() (*GetMapTaskResponse, error) {
	response := GetMapTaskResponse{}
	err := call("Master.GetMapTask", &GetMapTaskRequest{}, &response)
	return &response, err
}

func rpcGetReduceTask() (*GetReduceTaskResponse, error) {
	response := GetReduceTaskResponse{}
	err := call("Master.GetReduceTask", &GetReduceTaskRequest{}, &response)
	return &response, err
}

// func rpcCompleteMapTask() (*CompleteMapTaskResponse, error) {
// 	response := CompleteMapTaskResponse{}
// 	ok := call("Master.CompleteMapTask", &CompleteMapTaskRequest{}, &response)
// 	if !ok {
// 		return nil, errors.New("RPCCompleteMapTask failed")
// 	}
// 	fmt.Printf("RPCCompleteMapTask result: %+v\n", response)
// 	return &response, nil
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
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
