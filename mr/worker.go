package mr

import "fmt"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	wk := worker{}
	wk.mapf = mapf
	wk.reducef = reducef
	wk.register()
	wk.start()

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

func (wk *worker) register() {
	regArgs := RegisterArgs{}
	regReply := RegisterReply{}
	ok := call("Master.RegWorker", &regArgs, &regReply)

	if !ok {
		log.Fatal("Registeration Failed")
	}

	wk.id = regReply.WorkerId
	fmt.Println("Registeration Success, id is", wk.id)
}

func (wk *worker) start() {
	for {
		t := wk.taskReq()
		if !t.Alive {
			fmt.Println("The task is not alive")
			return
		}
		wk.taskDo(t)
	}

}

func (wk *worker) taskReq() Task {
	taskArgs := TaskArgs{}
	taskArgs.WorkerId = wk.id
	taskReply := TaskReply{}

	ok := call("Master.AssignTask", &taskArgs, &taskReply)
	if !ok {
		log.Fatal("Request Task failed")
	}
	fmt.Println("Complete task request")

	return taskReply.T
}

func (wk *worker) taskDo(t Task) {
	fmt.Println("Worker is doing the task Debug purpose")
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	fmt.Println(sockname)
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
