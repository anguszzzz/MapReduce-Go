package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
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
	//fmt.Println("Registeration Success, id is", wk.id)
}

func (wk *worker) start() {
	for {
		t := wk.taskReq()
		if !t.Alive {
			log.Printf("The task is not alive")
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
		log.Printf("Request Task failed")
		os.Exit(1)
	}
	//fmt.Println("Complete task request")

	return taskReply.T
}

func (wk *worker) taskDo(t Task) {
	//fmt.Println("Worker is doing the task Debug purpose")

	switch t.Phase {
	case MapPhase:
		//fmt.Println("Begin Map Phase")
		wk.doMapTask(t)
	case ReducePhase:
		//fmt.Println("Begin to do reduce Phase")
		wk.doReduceTask(t)

	default:
		panic("Worker is doing wrong tasks")
	}

}

// Do Map Task and using encoding function to encode the key-value pairs
func (wk *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.Filename)
	if err != nil {
		//fmt.Println("Error in open files")
		wk.feedback(t, false, err)
		return
	}

	kvs := wk.mapf(t.Filename, string(contents))
	reduces := make([][]KeyValue, t.NumReduce)

	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NumReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(t.Id, idx)
		f, err := os.Create(fileName)
		if err != nil {
			wk.feedback(t, false, err)
			return
		}

		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				wk.feedback(t, false, err)
				return
			}
		}

		if err := f.Close(); err != nil {
			wk.feedback(t, false, err)
			return
		}
	}

	wk.feedback(t, true, nil)
}

func (wk *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)

	for idx := 0; idx < t.NumMap; idx++ {
		fileName := reduceName(idx, t.Id)
		file, err := os.Open(fileName)
		if err != nil {
			//fmt.Println("Error opening files")
			wk.feedback(t, false, err)
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//fmt.Println("Error decoding maps")
				break
			}

			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	//fmt.Println("Complete reading and appending maps")
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, wk.reducef(k, v)))
	}
	//fmt.Println("Complete iterating maps")

	if err := ioutil.WriteFile(mergeName(t.Id), []byte(strings.Join(res, "")), 0600); err != nil {
		//fmt.Println("Error in reduce function")

		wk.feedback(t, false, err)
	}

	//fmt.Println("Complete One reduce function")
	wk.feedback(t, true, nil)
}

// Send feedback to the master, to tell the task is completed
// encounter error or something else
func (wk *worker) feedback(t Task, success bool, err error) {
	if err != nil {
		//log.Println("Encounter error with", err)
	}

	args := FeedbackTaskArgs{}
	args.Done = success
	args.Id = t.Id
	args.Phase = t.Phase
	args.WorkerId = wk.id
	reply := FeedbackTaskReply{}
	ok := call("Master.FeedbackTask", &args, &reply)

	if !ok {
		fmt.Println("Send task completion failed")
	}
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
	//fmt.Println(sockname)
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
