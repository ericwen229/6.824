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
			if err := handleTask(&requestTaskResponse, mapf, reducef); err != nil {
				log.Fatal("handle task failure: ", err)
			}
		case PENDING:
			log.Println("master pending for map tasks, waiting")
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
	reducef func(string, []string) string) error {
	if (requestTaskResponse.IsMapTask) {
		log.Printf("processing map task #%d", requestTaskResponse.TaskId)

		mapTask := requestTaskResponse.Task.MTask
		if err := handleMapTask(mapTask, mapf); err != nil {
			return err
		}
	} else {
		log.Printf("processing reduce task #%d", requestTaskResponse.TaskId)

		reduceTask := requestTaskResponse.Task.RTask
		if err := handleReduceTask(reduceTask, reducef); err != nil {
			return err
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
	return nil
}

//
// 读取文件所有内容
//
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

//
// 执行map任务
//
func handleMapTask(task *MapTask, mapf func(string, string) []KeyValue) error {
	// 读单个输入文件
	fileContent, err := readFile(task.InputFilePath)
	if err != nil {
		return err
	}

	// 执行map操作
	mapResult := mapf(task.InputFilePath, fileContent)

	bucketCount := task.NReduce
	buckets := make([][]KeyValue, bucketCount)

	// 分堆
	for _, pair := range mapResult {
		bucketNo := ihash(pair.Key) % bucketCount
		buckets[bucketNo] = append(buckets[bucketNo], pair)
	}

	// 写文件
	for iReduce := 0; iReduce < bucketCount; iReduce++ {
		outputFilePath := fmt.Sprintf(task.OutputFilePathTmpl, task.IMap, iReduce)
		bucket := buckets[iReduce]
		if err := writeKVPairs(bucket, outputFilePath); err != nil {
			return err
		}
	}

	return nil
}

//
// 执行reduce任务
//
func handleReduceTask(task *ReduceTask, reducef func(string, []string) string) error {
	var kvPairs []KeyValue

	// 读多个输入文件
	for _, inputFilePath := range task.InputFilePaths {
		pairs, err := readKVPairs(inputFilePath)
		if err != nil {
			return err
		}
		kvPairs = append(kvPairs, pairs...)
	}

	// 键值对按key排序
	sort.Sort(ByKey(kvPairs))

	outputFile, err := ioutil.TempFile(".", ".tmp-")
	defer outputFile.Close()
	if err != nil {
		return err
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

		// 写文件
		// 这里调用完集中写文件会好一些
		// 不过为了省事就用了框架代码中的实现
		_, err := fmt.Fprintf(outputFile, "%v %v\n", kvPairs[i].Key, output)
		if err != nil {
			return err
		}

		i = j
	}

	if err := os.Rename(outputFile.Name(), task.OutputFilePath); err != nil {
		return err
	}

	return nil
}

//
// 将json编码的键值对写入文件
//
func writeKVPairs(kvPairs []KeyValue, filePath string) error {
	file, err := ioutil.TempFile(".", ".tmp-")
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

	if err := os.Rename(file.Name(), filePath); err != nil {
		return err
	}

	return nil
}

//
// 从文件中读取json编码的键值对
//
func readKVPairs(filePath string) ([]KeyValue, error) {
	var kvPairs []KeyValue

	outputFile, err := os.Open(filePath)
	defer outputFile.Close()
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(outputFile)
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
