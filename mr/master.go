package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	workerId int
	mx       sync.Mutex
	taskChan chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.workerId += 1
	reply.WorkerId = m.workerId
	fmt.Println("RPC is passing")
	return nil
}

func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskChan
	reply.T = task

	fmt.Println("Master begin to assign task")
	if task.Alive {
		m.regTask(args, &task)
	}

	return nil
}

func (m *Master) regTask(args *TaskArgs, task *Task) {

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	//ret := true

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.mx = sync.Mutex{}

	if nReduce > len(files) {
		m.taskChan = make(chan Task, nReduce)
	} else {
		m.taskChan = make(chan Task, len(files))
	}

	// For test only
	task := Task{}
	m.taskChan <- task

	m.server()
	return &m
}
