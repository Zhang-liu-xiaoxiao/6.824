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
	//str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	//bytes := []byte(str)
	//var result []byte
	//rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	//for i := 0; i < 10; i++ {
	//	result = append(result, bytes[rand.Intn(len(bytes))])
	//}
	//name := string(result)
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {
		args := WorkerRequest{}
		reply := MasterResponse{}
		args.RequestType = AskTask
		ok := call("Coordinator.ReceiveRequest", &args, &reply)
		if !ok {
			fmt.Printf("Ask task call failed!\n")
		}

		if reply.FinishMark {
			break
		}

		if reply.TaskType == Map {

		} else if reply.TaskType == Reduce {

		}
	}
}

func ProcessMapTask(filename string, mapf func(string, string) []KeyValue, taskSequence int, nReduce int) {
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outFiles[i], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncs[i] = json.NewEncoder(outFiles[i])
	}

	for _, value := range intermediate {
		index := ihash(value.Key) % nReduce
		err := fileEncs[index].Encode(value)
		if err != nil {
			log.Fatalf("Map task %d Encode fail ", taskSequence)
		}
	}
	for i := 0; i < nReduce; i++ {
		realFileName := fmt.Sprintf("mr-%d-%d", taskSequence, i)
		err := os.Rename(outFiles[i].Name(), realFileName)
		if err != nil {
			log.Fatalf("rename file error file :%s to %s", outFiles[i].Name(), realFileName)
		}
		err = outFiles[i].Close()
		if err != nil {
			log.Printf("Close file error :%s", outFiles[i].Name())
		}
	}

	UpdateFinishTask(taskSequence, Map)
}

func UpdateFinishTask(taskSequence int, taskType TaskEnum) {
	args := WorkerRequest{}
	reply := MasterResponse{}
	args.TaskSequence = taskSequence
	args.RequestType = FinishTask
	args.TaskType = taskType
	ok := call("Coordinator.ReceiveRequest", &args, &reply)
	if !ok {
		fmt.Printf("Finish task call failed! type:%v\n", taskType)
	}
}

func ProcessReduceTask(files []string, reducef func(string, []string) string, reduceSequence int) {

	var intermediate []KeyValue
	for i := 0; i < len(files); i++ {
		file, err := os.Open(files[i])
		if err != nil {
			log.Fatalf("Reduce task %d read file:%s error", reduceSequence, files[i])
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("Reduce task %d Error decode kv", reduceSequence)
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	tempFile, err := ioutil.TempFile("mr-out-temp", "mr-out-*")
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

	oname := fmt.Sprintf("mr-out-%d", reduceSequence)
	err = os.Rename(tempFile.Name(), oname)
	if err != nil {
		log.Fatalf("rename file error file :%s to %s", tempFile.Name(), oname)
	}
	tempFile.Close()

	UpdateFinishTask(reduceSequence, Reduce)
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

	fmt.Println(err)
	return false
}
