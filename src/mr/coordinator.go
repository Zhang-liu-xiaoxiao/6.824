package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkingEnum int
type MasterStatusEnum int

const (
	Idle WorkingEnum = iota + 1
	Working
	Finish
)

const (
	Mapping MasterStatusEnum = iota + 1
	Reducing
	Done
)

type Coordinator struct {
	// Your definitions here.
	MapTasks               []MapTask
	MapTaskLock            sync.Mutex
	ReduceTasks            []ReduceTask
	ReduceTaskLock         sync.Mutex
	MapTaskNums            int
	ReduceTaskNums         int
	FinishedMapTaskNums    int
	FinishedReduceTaskNums int
	WorkingStatus          MasterStatusEnum
	WorkingStatusLock      sync.Mutex
}

type MapTask struct {
	SequenceId int
	FileName   string
	TaskStatus WorkingEnum
}

type ReduceTask struct {
	SequenceId int
	FileNames  []string
	TaskStatus WorkingEnum
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReceiveRequest(args *WorkerRequest, reply *MasterResponse) error {
	//fmt.Println("master recieve request %v", args)
	c.ProcessRequest(args, reply)
	return nil
}

func (c *Coordinator) ProcessRequest(args *WorkerRequest, reply *MasterResponse) {
	c.WorkingStatusLock.Lock()
	if c.WorkingStatus == Done {
		reply.FinishMark = true
		return
	}
	c.WorkingStatusLock.Unlock()
	if args.RequestType == AskTask {
		// schedule a task, map or reduce
		if c.TryAssignMapTask(reply) {
			return
		}
		if c.TryAssignReduceTask(reply) {
			return
		}
		// Waiting for last few  Reducing tasks finish
		// just do nothing
		reply.Bubble = true
		return

	} else if args.RequestType == FinishTask {
		if args.TaskType == Map {
			c.FinishMapTask(args.TaskSequence)
		} else if args.TaskType == Reduce {
			c.FinishReduceTask(args.TaskSequence)
		}
	}
}

func (c *Coordinator) TryAssignReduceTask(reply *MasterResponse) bool {
	c.WorkingStatusLock.Lock()
	defer c.WorkingStatusLock.Unlock()
	if c.WorkingStatus == Reducing {
		// Mapping finished! assign Reduce Task
		c.ReduceTaskLock.Lock()
		defer c.ReduceTaskLock.Unlock()
		for i, reduceTask := range c.ReduceTasks {
			if reduceTask.TaskStatus == Idle {
				reply.TaskSequence = reduceTask.SequenceId
				reply.TaskType = Reduce
				reply.NReduce = c.ReduceTaskNums
				reply.FileName = reduceTask.FileNames
				reply.FinishMark = false
				c.ReduceTasks[i].TaskStatus = Working
				go func(i int) {
					timer := time.NewTimer(10 * time.Second)
					<-timer.C
					c.ReduceTaskLock.Lock()
					defer c.ReduceTaskLock.Unlock()
					if c.ReduceTasks[i].TaskStatus != Finish {
						c.ReduceTasks[i].TaskStatus = Idle
					}
				}(i)
				return true
			}
		}
	}
	return false
}

func (c *Coordinator) TryAssignMapTask(reply *MasterResponse) (res bool) {
	c.MapTaskLock.Lock()
	defer c.MapTaskLock.Unlock()
	for i, task := range c.MapTasks {
		if task.TaskStatus == Idle {
			reply.TaskSequence = task.SequenceId
			reply.TaskType = Map
			reply.NReduce = c.ReduceTaskNums
			//reply.FileName[0] = task.FileName
			reply.FileName = append(reply.FileName, task.FileName)
			reply.FinishMark = false
			c.MapTasks[i].TaskStatus = Working
			go func(i int) {
				timer := time.NewTimer(10 * time.Second)
				<-timer.C
				c.MapTaskLock.Lock()
				defer c.MapTaskLock.Unlock()
				if c.MapTasks[i].TaskStatus != Finish {
					c.MapTasks[i].TaskStatus = Idle
				}
			}(i)
			return true
		}
	}
	return false
}
func (c *Coordinator) FinishMapTask(taskNum int) {
	c.MapTaskLock.Lock()
	defer c.MapTaskLock.Unlock()
	c.MapTasks[taskNum].TaskStatus = Finish
	c.FinishedMapTaskNums++
	//fmt.Println("Master Finish Map Task", taskNum, "cur finished Map ", c.FinishedMapTaskNums)
	if c.FinishedMapTaskNums >= c.MapTaskNums {
		c.WorkingStatusLock.Lock()
		c.WorkingStatus = Reducing
		c.WorkingStatusLock.Unlock()
	}
}

func (c *Coordinator) FinishReduceTask(taskNum int) {
	c.ReduceTaskLock.Lock()
	defer c.ReduceTaskLock.Unlock()
	c.ReduceTasks[taskNum].TaskStatus = Finish
	c.FinishedReduceTaskNums++
	//fmt.Println("Master Finish Reduce Task", taskNum, "cur finished Reduce ", c.FinishedReduceTaskNums)

	if c.FinishedReduceTaskNums >= c.ReduceTaskNums {
		c.WorkingStatusLock.Lock()
		//fmt.Println("Finished all reduce task!,finished num:", c.FinishedReduceTaskNums, "need num:", c.ReduceTaskNums)
		c.WorkingStatus = Done
		c.WorkingStatusLock.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.ReduceTaskLock.Lock()
	if c.FinishedReduceTaskNums == c.ReduceTaskNums {
		ret = true
	}
	defer c.ReduceTaskLock.Unlock()
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTasks = make([]MapTask, len(files))
	c.ReduceTasks = make([]ReduceTask, nReduce)
	c.ReduceTaskNums = nReduce
	c.MapTaskNums = len(files)
	c.WorkingStatus = Mapping
	// Your code here.
	for i := range c.MapTasks {
		c.MapTasks[i].TaskStatus = Idle
		c.MapTasks[i].FileName = files[i]
		c.MapTasks[i].SequenceId = i
	}

	//var reduceFilesArray [][]string
	reduceFilesArray := make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		for j := 0; j < len(files); j++ {
			reduceFilesArray[i] = append(reduceFilesArray[i], fmt.Sprintf("mr-%d-%d", j, i))
		}
	}
	for i := range c.ReduceTasks {
		c.ReduceTasks[i].FileNames = reduceFilesArray[i]
		c.ReduceTasks[i].SequenceId = i
		c.ReduceTasks[i].TaskStatus = Idle
	}
	//fmt.Printf("files %v \n", files)
	//fmt.Printf("map tasks %v \n", c.MapTasks)
	//fmt.Printf("reduce  tasks %v \n", c.ReduceTasks)
	c.server()
	return &c
}
