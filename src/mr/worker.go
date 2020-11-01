package mr

import "log"
import "net/rpc"
import "hash/fnv"
import "time"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		var requestTaskResponse RequestTaskResponse
		if !call("Master.RequestTask", &RequestTaskRequest{}, &requestTaskResponse) {
			log.Println("fail to request task from master, aborting")
			return
		}

		switch (requestTaskResponse.Code) {
		case ERROR:
			log.Println("error response from master, aborting")
			return
		case SUCCESS:
			handleTask(&requestTaskResponse, mapf, reducef)
		case PENDING:
			log.Println("master pending for map tasks, waiting")
			continue
		case DONE:
			log.Println("all tasks complete, exiting")
			return
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func handleTask(
	requestTaskResponse *RequestTaskResponse,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if (requestTaskResponse.IsMapTask) {
		log.Printf("processing map task #%d", requestTaskResponse.TaskId)

		mapTask := requestTaskResponse.Task.MTask
		if !handleMapTask(mapTask, mapf) {
			return
		}
	} else {
		log.Printf("processing reduce task #%d", requestTaskResponse.TaskId)

		reduceTask := requestTaskResponse.Task.RTask
		if !handleReduceTask(reduceTask, reducef) {
			return
		}
	}

	var completeTaskResponse CompleteTaskResponse
	call(
		"Master.CompleteTask",
		&CompleteTaskRequest{
			IsMapTask: requestTaskResponse.IsMapTask,
			TaskId: requestTaskResponse.TaskId,
		},
		&completeTaskResponse)
}

func handleMapTask(task *MapTask, mapf func(string, string) []KeyValue) bool {
	// TODO
	return true
}

func handleReduceTask(task *ReduceTask, reducef func(string, []string) string) bool {
	// TODO
	return true
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
