package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
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

// Add your RPC definitions here.
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

type FeedbackTaskArgs struct {
	Done     bool
	Id       int
	Phase    TaskPhase
	WorkerId int
}

type FeedbackTaskReply struct {
}

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
