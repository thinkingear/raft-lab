package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	// paper
	state State
	// private
	filenames []string
	taskType  MapOrReduce
	taskId    int
	workerId  int
}

type Coordinator struct {
	// Your definitions here.

	// shared data
	stage   MapOrReduce
	cond    sync.Cond
	ch      chan *Task
	tasks   []Task
	workers []WorkerStruct
	// private data
	intermediateFiles    [][]string
	nMap                 int
	nReduce              int
	workerId             int
	completedMapTasks    int
	completedReduceTasks int
}

func (c *Coordinator) createWorkerId() int {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	c.workers[c.workerId] = WorkerStruct{
		taskId:           -1,
		taskType:         -1,
		workerId:         c.workerId,
		state:            Working,
		stateUpdatedTime: time.Now().Unix(),
		recoverChan:      make(chan int, 1)}

	temp := c.workerId
	c.workerId++
	return temp
}

func (c *Coordinator) daemonThread(workerId int) {
	c.cond.L.Lock()
	worker := &c.workers[workerId]

	for {
		gap := TimeOver
		for gap > 0 {
			c.cond.L.Unlock()
			time.Sleep(time.Duration(gap) * time.Second)
			c.cond.L.Lock()
			gap = TimeOver - (time.Now().Unix() - worker.stateUpdatedTime)

			//if worker.taskId >= 0 {
			//	task := &c.tasks[c.workers[workerId].taskId]
			//	fmt.Printf("timer reset:\tworkerId = %d, gap = %d, taskId = %d, taskType = %d, taskState = %d, workerState = %d\n", workerId, gap, task.taskId, task.taskType, task.state, worker.state)
			//}
		}

		if worker.taskId < 0 && worker.state == Waiting {
			continue
		}

		task := &c.tasks[worker.taskId]
		if task.state == InProgress && worker.state == Working {
			//fmt.Printf("task faild:\tworkerId = %d, taskId = %d, taskType = %d\n", workerId, c.workers[workerId].taskId, worker.taskType)

			worker.state = Failed
			task.state = Idle
			c.ch <- task
			c.cond.Broadcast()
			c.cond.L.Unlock()
			<-worker.recoverChan
			c.cond.L.Lock()
		}
	}
}

func (c *Coordinator) ConnectionCall(dummyArgs DummyArgs, reply *ConnectionReply) error {
	workerId := c.createWorkerId()
	go c.daemonThread(workerId)

	reply.WorkerId = workerId
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) TaskCompletedCall(args TaskCompletedArgs, dummyArgs *DummyArgs) error {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	worker := &c.workers[args.WorkerId]
	defer func() { worker.stateUpdatedTime = time.Now().Unix() }()

	//fmt.Printf("commit request:\tworkerId = %d, taskId = %d, taskType = %d\n", worker.workerId, worker.taskId, worker.taskType)

	var task *Task = nil
	// find the worker's corresponding task
	if worker.state != Failed {
		task = &c.tasks[args.TaskId]
	} else {
		worker.state = Working
		worker.recoverChan <- 0
		return nil
	}

	task.state = Completed

	//fmt.Printf("commit ok:\tworkerId = %d, taskId = %d, taskType = %d\n", task.workerId, task.taskId, task.taskType)
	//fmt.Printf("commit list:\t")
	//for _, task := range c.tasks {
	//	if task.state == Completed {
	//		fmt.Printf("taskId = %d, ", task.taskId)
	//	}
	//}
	//fmt.Println()

	if c.stage == RType {
		c.completedReduceTasks++
		return nil
	} else {
		c.completedMapTasks++
	}

	// if a Map Task has completed
	// add the output intermediate file to the matrix
	for reduceTaskId, filename := range args.Filenames {
		if filename != "" {
			c.intermediateFiles[task.taskId][reduceTaskId] = filename
		}
	}

	// if all the map tasks have completed
	if c.completedMapTasks == c.nMap {
		c.stage = RType
		//fmt.Println()
		//fmt.Println("Stage Change!!!!!!!!!!!!!!!!!!!!!!")
		//fmt.Println("Stage Change!!!!!!!!!!!!!!!!!!!!!!")
		//fmt.Println("Stage Change!!!!!!!!!!!!!!!!!!!!!!")
		//fmt.Println()

		for i := 0; i < c.nReduce; i++ {
			task = &c.tasks[i]
			task.taskId = i
			task.state = Idle
			task.taskType = RType
			task.filenames = make([]string, c.nMap)

			for j := 0; j < c.nMap; j++ {
				task.filenames[j] = c.intermediateFiles[j][i]
			}

			c.ch <- task
		}

		c.cond.Broadcast()
	} else if c.completedReduceTasks == c.nReduce {
		c.cond.Broadcast()
	}

	return nil
}

func (c *Coordinator) TaskAssignCall(args TaskAssignArgs, reply *TaskAssignReply) error {

	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	worker := &c.workers[args.WorkerId]

	defer func() { worker.stateUpdatedTime = time.Now().Unix() }()

	//fmt.Printf("assign request:\tworkerId = %d\n", worker.workerId)

	// if there's an idle task
	for len(c.ch) == 0 {
		worker.state = Waiting
		c.cond.Wait()
	}

	// if job has done
	if c.completedReduceTasks == c.nReduce && c.stage == RType {
		reply.JobDone = true
		return nil
	}

	// assign an idle task
	task := <-c.ch
	// update a bunch of data information
	task.state = InProgress
	task.workerId = worker.workerId

	worker.state = Working
	worker.taskId = task.taskId
	worker.taskType = task.taskType

	reply.Filenames = task.filenames
	reply.TaskId = task.taskId
	reply.TaskType = c.stage

	//fmt.Printf("assign ok:\tworkerId = %d, taskId = %d, taskType = %d\n", task.workerId, task.taskId, task.taskType)
	return nil
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
	// Your code here.
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	if c.completedReduceTasks == c.nReduce && c.stage == RType {
		return true
	} else {
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		stage:   MType,
		nReduce: nReduce,
		nMap:    len(files),
		workers: make([]WorkerStruct, 1000),
	}

	max := func(x, y int) int {
		if x > y {
			return x
		}
		return y
	}

	c.ch = make(chan *Task, max(c.nReduce, c.nMap))

	c.cond.L = new(sync.Mutex)
	c.intermediateFiles = make([][]string, c.nMap)

	c.tasks = make([]Task, max(c.nMap, c.nReduce))
	for i := 0; i < c.nMap; i++ {
		c.intermediateFiles[i] = make([]string, c.nReduce)
		task := &c.tasks[i]
		task.filenames = []string{files[i]}
		task.taskId = i
		task.state = Idle
		task.taskType = MType
		c.ch <- task
	}

	c.server()
	return &c
}
