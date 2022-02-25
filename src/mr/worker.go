package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % nReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerStruct struct {
	workerId, nReduce int
	taskId            int
	taskType          MapOrReduce
	state             State
	stateUpdatedTime  int64
	recoverChan       chan int
}

func makeWorker() *WorkerStruct {
	worker := WorkerStruct{}
	reply := ConnectionReply{}
	ok := call("Coordinator.ConnectionCall", DummyArgs{}, &reply)
	if ok {
		worker.nReduce = reply.NReduce
		worker.workerId = reply.WorkerId
	} else {
		log.Fatalf("cannot establish a connection")
	}

	return &worker
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// step 1: establish a connection from worker to the master
	worker := makeWorker()
	for {
		reply := TaskAssignReply{}
		args := TaskAssignArgs{WorkerId: worker.workerId}
		// step 2: assign task
		call("Coordinator.TaskAssignCall", args, &reply)
		// step 3: do assigned task
		if reply.JobDone {
			//fmt.Println("all tasks have completed, worker exit")
			os.Exit(0)
		} else if reply.TaskType == MType {
			// step 3.1: do map task
			worker.DoMapTask(mapf, &reply)
		} else {
			// step 3.2: do reduce task
			worker.DoReduceTask(reducef, &reply)
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
		//log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

func (w *WorkerStruct) DoMapTask(mapf func(string, string) []KeyValue, reply *TaskAssignReply) {
	// do map task
	filename := reply.Filenames[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	args := TaskCompletedArgs{
		TaskId:    reply.TaskId,
		Filenames: make([]string, w.nReduce),
		TaskType:  MType,
		WorkerId:  w.workerId,
	}

	kva := mapf(filename, string(content))
	encFiles := make([]*json.Encoder, w.nReduce)
	for _, kv := range kva {
		i := ihash(kv.Key) % w.nReduce
		if encFiles[i] == nil {
			tempFile, _ := ioutil.TempFile("./", fmt.Sprintf("mr-%d-%d-", reply.TaskId, i))
			encFiles[i] = json.NewEncoder(tempFile)

			intermediateFilename := fmt.Sprintf("./mr-%d-%d", reply.TaskId, i)
			args.Filenames[i] = intermediateFilename

			defer tempFile.Close()
			defer os.Rename(tempFile.Name(), intermediateFilename)
		}
		encFiles[i].Encode(&kv)
	}

	// commit the task
	ok := call("Coordinator.TaskCompletedCall", args, &DummyArgs{})

	if !ok {
		log.Fatalf("Failed to call TaskCompletedCall()")
	}

}

func (w *WorkerStruct) DoReduceTask(reducef func(string, []string) string, reply *TaskAssignReply) {

	args := TaskCompletedArgs{
		TaskId:   reply.TaskId,
		TaskType: RType,
		WorkerId: w.workerId,
	}

	defer call("Coordinator.TaskCompletedCall", args, &DummyArgs{})

	// put the data into kva
	kva := []KeyValue{}
	for _, filename := range reply.Filenames {
		if filename == "" {
			continue
		}

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	if len(kva) == 0 {
		return
	}

	// sort kva by key
	sort.Sort(ByKey(kva))

	// do reduce task
	tempFile, err := ioutil.TempFile("./", fmt.Sprintf("mr-out-%d-", reply.TaskId))
	if err != nil {
		log.Fatalf("cannot create the temporary file")
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	os.Rename(tempFile.Name(), fmt.Sprintf("./mr-out-%d", reply.TaskId))
	tempFile.Close()
}
