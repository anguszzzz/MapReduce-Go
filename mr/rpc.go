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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	T Task
}

type TaskPhase int

type Task struct {
	Filename  string
	NumReduce int
	NumMap    int
	Id        int
	Phase     TaskPhase
	Alive     bool
}

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
