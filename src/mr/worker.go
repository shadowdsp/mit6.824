package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"strings"

	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"

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

func saveKV(kvs []KeyValue, file *os.File) error {
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
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

func createFile(filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		_, err = os.Create(filename)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveMapTask(
	mapf func(string, string) []KeyValue,
	mapTaskID int,
	filenames []string,
	nReduce int,
) ([]string, error) {
	// execute map task
	intermediateKVs := make([][]KeyValue, nReduce)
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

		kvs := mapf(filename, string(content))
		for _, kv := range kvs {
			reduceID := ihash(kv.Key) % nReduce
			intermediateKVs[reduceID] = append(intermediateKVs[reduceID], kv)
		}
	}

	tmpOutputPaths := []string{}
	uid := uuid.New().String()[:8]
	for reduceID, kvs := range intermediateKVs {
		interFilename := fmt.Sprintf("mr-%v-%v-%v", mapTaskID, reduceID, uid)
		err := createFile(interFilename)
		if err != nil {
			return nil, err
		}
		ofile, err := os.OpenFile(interFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}

		err = saveKV(kvs, ofile)
		if err != nil {
			return nil, err
		}
		ofile.Close()
		tmpOutputPaths = append(tmpOutputPaths, interFilename)
	}

	outputPaths := []string{}
	for _, src := range tmpOutputPaths {
		dst := strings.TrimSuffix(src, "-"+uid)
		log.Debugf("[Worker.resolveMapTask] src: %v, dst: %v", src, dst)
		os.Rename(src, dst)
		outputPaths = append(outputPaths, dst)
	}
	return outputPaths, nil
}

func resolveReduceTask(
	reducef func(string, []string) string,
	reduceTaskID int,
	filepaths []string,
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
	log.Debugf("[Worker.resolveReduceTask] reduceTaskID: %v, filepath: %+v", reduceTaskID, filepaths)
	sort.Sort(ByKey(kvs))

	uid := uuid.New().String()[:8]
	outputFilepath := fmt.Sprintf("mr-out-%v", reduceTaskID)
	tmpFilepath := fmt.Sprintf("%v-%v", outputFilepath, uid)
	ofile, err := os.Create(tmpFilepath)
	if err != nil {
		return "", err
	}
	defer ofile.Close()

	log.Debugf("[Worker.resolveReduceTask] len(kvs): %v", len(kvs))
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

	log.Debugf("[Worker.resolveReduceTask] src: %v, dst: %v", tmpFilepath, outputFilepath)
	os.Rename(tmpFilepath, outputFilepath)
	return outputFilepath, nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		getTaskReq := GetTaskRequest{}
		getTaskResp := GetTaskResponse{}
		ok := call("Master.GetTask", &getTaskReq, &getTaskResp)
		if !ok {
			os.Exit(1)
		}
		log.Infof("worker get task resp: %+v", getTaskResp)

		completeTaskReq := CompleteTaskRequest{
			TaskID:   getTaskResp.TaskID,
			TaskType: getTaskResp.TaskType,
		}
		switch getTaskResp.TaskType {
		case TaskTypeMap:
			outputPaths, err := resolveMapTask(mapf, getTaskResp.TaskID, getTaskResp.TaskInputs, getTaskResp.NReduce)
			if err != nil {
				log.Errorf("resolveMapTask failed, err: %v", err)
				continue
			}
			completeTaskReq.TaskOutputs = outputPaths
		case TaskTypeReduce:
			outputPath, err := resolveReduceTask(reducef, getTaskResp.TaskID, getTaskResp.TaskInputs)
			if err != nil {
				log.Errorf("resolveReduceTask failed, err: %v", err)
				continue
			}
			completeTaskReq.TaskOutputs = []string{outputPath}
		default:
			time.Sleep(1000 * time.Millisecond)
			continue
		}
		log.Infof("worker complete task req: %+v", completeTaskReq)
		completeTaskResp := CompleteTaskResponse{}
		ok = call("Master.CompleteTask", &completeTaskReq, &completeTaskResp)
		if !ok {
			os.Exit(1)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	log.Errorf(err.Error())
	return false
}
