package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
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
type ByKey []KeyValue

// for sorting by key.
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {
		args := WorkerRequest{}
		reply := MasterResponse{}
		args.RequestType = AskTask
		ok := call("Coordinator.ReceiveRequest", &args, &reply)
		if !ok {
			fmt.Printf("Ask task call failed!\n")
			break
		}
		//fmt.Printf("master response %v\n", reply)
		if reply.FinishMark == true {
			break
		}
		if reply.Bubble == true {
			continue
		}
		if reply.TaskType == Map {
			fmt.Printf("Worker recieve Map Task %d\n", reply.TaskSequence)
			ProcessMapTask(reply, mapf)
		} else if reply.TaskType == Reduce {
			fmt.Printf("Worker recieve Reduce Task %d\n", reply.TaskSequence)
			ProcessReduceTask(reply, reducef)
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func ProcessMapTask(reply MasterResponse, mapf func(string, string) []KeyValue) {
	var intermediate []KeyValue

	file, err := os.Open(reply.FileName[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName[0])
	}
	defer file.Close()
	kva := mapf(reply.FileName[0], string(content))
	intermediate = append(intermediate, kva...)

	nReduce := reply.NReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outFiles[i], err = ioutil.TempFile("/tmp", "mr-temp")
		if err != nil {
			fmt.Printf("error create temp files:  %v\n", err)

		}
		fileEncs[i] = json.NewEncoder(outFiles[i])
	}
	//json.Marshal()
	for _, value := range intermediate {
		index := ihash(value.Key) % nReduce
		err := fileEncs[index].Encode(&value)
		if err != nil {
			log.Fatalf("Map task %d Encode fail error:%v", reply.TaskSequence, err)
		}
	}
	for i := 0; i < reply.NReduce; i++ {
		realFileName := fmt.Sprintf("mr-%d-%d", reply.TaskSequence, i)
		err := os.Rename(outFiles[i].Name(), realFileName)
		if err != nil {
			log.Fatalf("rename file error file :%s to %s", outFiles[i].Name(), realFileName)
		}
		err = outFiles[i].Close()
		if err != nil {
			log.Printf("Close file error :%s", outFiles[i].Name())
		}
	}

	UpdateFinishTask(reply.TaskSequence, Map)
}

func UpdateFinishTask(taskSequence int, taskType TaskEnum) {
	args := WorkerRequest{}
	reply := MasterResponse{}
	args.TaskSequence = taskSequence
	args.RequestType = FinishTask
	args.TaskType = taskType
	ok := call("Coordinator.ReceiveRequest", &args, &reply)
	if taskType == Map {
		fmt.Printf("worker recieve finish map response %d\n", taskSequence)
	} else {
		fmt.Printf("worker recieve finish reduce response %d\n", taskSequence)
	}
	if !ok {
		fmt.Printf("worker Finish task call failed! type:%v\n", taskType)
	}
}

func ProcessReduceTask(reply MasterResponse, reducef func(string, []string) string) {

	var intermediate []KeyValue
	for i := 0; i < len(reply.FileName); i++ {
		file, err := os.Open(reply.FileName[i])
		if err != nil {
			log.Fatalf("Reduce task %d read file:%s error", reply.TaskSequence, reply.FileName[i])
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("Reduce task %d Error decode kv", reply.TaskSequence)
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	tempFile, err := ioutil.TempFile("/tmp", "mr-out-*")
	fmt.Printf("temp file name :%s\n", tempFile.Name())
	if err != nil {
		return
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	oname := fmt.Sprintf("mr-out-%d", reply.TaskSequence)
	err = os.Rename(tempFile.Name(), oname)
	if err != nil {
		log.Fatalf("rename file error file :%s to %s", tempFile.Name(), oname)
	}
	tempFile.Close()

	UpdateFinishTask(reply.TaskSequence, Reduce)
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("rpc error %v\n", err)
	return false
}
