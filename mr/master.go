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

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

// Task status to record tasks' status, it will show the worker who worked on this task
// the runing, ready, complete, waiting status and also the start time for controlling
// the life cycle

type TaskStatus struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

// should have mutex lock for handling race condition, task channal to assign tasks and
// record workerid, taskphases and also files names and done or not.
type Master struct {
	// Your definitions here.
	workerId  int
	mx        sync.Mutex
	taskChan  chan Task
	taskPhase TaskPhase
	taskStats []TaskStatus
	files     []string
	nReduce   int
	done      bool
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

// Register a worker and assign unique workerid to it
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.workerId += 1
	reply.WorkerId = m.workerId
	fmt.Println("RPC is passing")
	return nil
}

// Assign a task to the requesting worker
func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskChan
	reply.T = task

	fmt.Println("Master begin to assign task")
	if task.Alive {
		m.regTask(args, &task)
	}

	return nil
}

// Register a task before assign it to the worker, called by the assigntask func
func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mx.Lock()
	defer m.mx.Unlock()

	if m.taskPhase != task.Phase {
		panic("Request task phase failed")
	}

	m.taskStats[task.Id].Status = TaskStatusRunning
	m.taskStats[task.Id].WorkerId = args.WorkerId
	m.taskStats[task.Id].StartTime = time.Now()
}

// Do mapper tasks first
func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStatus, len(m.files))
}

// Timer to control the assign task interval
func (m *Master) tickTimer() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

// Create the tasks and make schedule
func (m *Master) schedule() {
	m.mx.Lock()
	defer m.mx.Unlock()

	if m.done {
		return
	}

	finished := true
	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady:
			finished = false
			m.taskChan <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			finished = false
		case TaskStatusRunning:
			finished = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskChan <- m.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			m.taskStats[index].Status = TaskStatusQueue
			m.taskChan <- m.getTask(index)
		default:
			panic("Task status error")
		}
	}

	if finished {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) getTask(taskId int) Task {

	return Task{}
}

func (m *Master) initReduceTask() {

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
	//ret := false

	// Your code here.
	m.mx.Lock()
	defer m.mx.Unlock()

	return m.done
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

	m.initMapTask()
	go m.tickTimer()

	// For test only
	task := Task{}
	m.taskChan <- task

	m.server()
	return &m
}
