package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func readFile(path string) (string, error) {
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		return "", err
	}
	
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	} else {
		return string(content), nil
	}
}

func handleMapTask(task *MapTask, mapf func(string, string) []KeyValue) bool {
	fileContent, err := readFile(task.InputFilePath)
	if err != nil {
		return false
	}

	mapResult := mapf(task.InputFilePath, fileContent)

	bucketCount := task.NReduce
	buckets := make([][]KeyValue, bucketCount)

	for _, pair := range mapResult {
		bucketNo := ihash(pair.Key) % bucketCount
		buckets[bucketNo] = append(buckets[bucketNo], pair)
	}

	for iReduce := 0; iReduce < bucketCount; iReduce++ {
		outputFilePath := fmt.Sprintf(task.OutputFilePathTmpl, task.IMap, iReduce)
		bucket := buckets[iReduce]
		if err := writeKVPairs(bucket, outputFilePath); err != nil {
			return false
		}
	}

	return true
}

func handleReduceTask(task *ReduceTask, reducef func(string, []string) string) bool {
	var kvPairs []KeyValue
	for _, inputFilePath := range task.InputFilePaths {
		pairs, err := readKVPairs(inputFilePath)
		if err != nil {
			return false
		}
		kvPairs = append(kvPairs, pairs...)
	}

	sort.Sort(ByKey(kvPairs))

	outputFile, err := os.Create(task.OutputFilePath)
	defer outputFile.Close()
	if err != nil {
		return false
	}

	i := 0
	for i < len(kvPairs) {
		// 找到key相同的下标区间[i,j)
		j := i + 1
		for j < len(kvPairs) && kvPairs[j].Key == kvPairs[i].Key {
			j++
		}

		// 提取所有的value
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvPairs[k].Value)
		}

		// 调用reduce
		output := reducef(kvPairs[i].Key, values)

		// 输出结果
		_, err := fmt.Fprintf(outputFile, "%v %v\n", kvPairs[i].Key, output)
		if err != nil {
			return false
		}

		i = j
	}
	return true
}

func writeKVPairs(kvPairs []KeyValue, filePath string) error {
	file, err := os.Create(filePath)
	defer file.Close()
	if err != nil {
		return err
	}

	enc := json.NewEncoder(file)
	for _, kv := range kvPairs {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}

	return nil
}

func readKVPairs(filePath string) ([]KeyValue, error) {
	var kvPairs []KeyValue

	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvPairs = append(kvPairs, kv)
	}

	return kvPairs, nil
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
