package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type State int
type MapOrReduce int

const (
	MType = 1
	RType = 2
	// for task state
	InProgress State = 3
	Idle       State = 4
	Completed  State = 5
	// for worker state
	Working  State = 6
	Failed   State = 7
	Waiting  State = 8
	TimeOver int64 = 8
)

type DummyArgs struct{}

type TaskAssignReply struct {
	//NoIdleTask bool
	JobDone   bool
	TaskType  MapOrReduce
	Filenames []string
	TaskId    int
}

type TaskCompletedArgs struct {
	Filenames []string
	TaskId    int
	TaskType  MapOrReduce
	WorkerId  int
}

type TaskAssignArgs struct {
	WorkerId int
}

type ConnectionReply struct {
	NReduce  int
	WorkerId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
