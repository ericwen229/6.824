package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func iHash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	lastCompleteTaskId := -1
	for {
		req := &RequestTaskReq{
			lastCompleteTaskId,
		}
		resp := &RequestTaskResp{}

		err := call("Coordinator.RequestTask", req, resp)
		if err != nil {
			panic(fmt.Sprintf("failed to contact coordinator: %s", err.Error()))
		}

		if resp.IsDone {
			break
		}

		if resp.Task == nil {
			// not done yet, wait
			time.Sleep(time.Second)
		} else {
			err := handleTask(resp.Task, mapF, reduceF)
			if err == nil {
				lastCompleteTaskId = resp.Task.ID
			} else {
				lastCompleteTaskId = -1
			}
		}
	}
}

func handleTask(
	task *Task,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) error {
	if task.Type == TaskMap {
		return handleMap(task.MapTask, mapF)
	} else if task.Type == TaskReduce {
		return handleReduce(task.ReduceTask, reduceF)
	} else {
		panic(fmt.Sprintf("unknown task type %d", task.Type))
	}
}

func handleMap(task *MapTask, mapF func(string, string) []KeyValue) error {
	file, err := os.Open(task.File)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	intermediateKvs := mapF(task.File, string(content))

	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediateKvs {
		bucketIndex := iHash(kv.Key) % task.NReduce
		buckets[bucketIndex] = append(buckets[bucketIndex], kv)
	}

	for iReduce, bucket := range buckets {
		err = writeBucket(makeIntermediateFileName(task.IMap, iReduce), bucket)
		if err != nil {
			return err
		}
	}
	return nil
}

func handleReduce(task *ReduceTask, reduceF func(string, []string) string) error {
	var fileNames []string
	for iMap := 0; iMap < task.NMap; iMap++ {
		fileNames = append(fileNames, makeIntermediateFileName(iMap, task.IReduce))
	}
	var intermediateKvs []KeyValue
	for _, fileName := range fileNames {
		bucket, err := readBucket(fileName)
		if err != nil {
			return err
		}
		intermediateKvs = append(intermediateKvs, bucket...)
	}

	sort.Sort(ByKey(intermediateKvs))

	var outContent bytes.Buffer
	subsequenceStart := 0
	for subsequenceStart < len(intermediateKvs) {
		// find subsequence start and end
		subsequenceEnd := subsequenceStart + 1
		for subsequenceEnd < len(intermediateKvs) && intermediateKvs[subsequenceEnd].Key == intermediateKvs[subsequenceStart].Key {
			subsequenceEnd++
		}

		// collect subsequence
		var subsequence []string
		for iSubSequence := subsequenceStart; iSubSequence < subsequenceEnd; iSubSequence++ {
			subsequence = append(subsequence, intermediateKvs[iSubSequence].Value)
		}

		output := reduceF(intermediateKvs[subsequenceStart].Key, subsequence)
		outContent.WriteString(fmt.Sprintf("%s %s\n", intermediateKvs[subsequenceStart].Key, output))

		subsequenceStart = subsequenceEnd
	}

	return atomicWriteFile(makeFinalFileName(task.IReduce), outContent.Bytes())
}

func makeIntermediateFileName(iMap, iReduce int) string {
	return fmt.Sprintf("mr-%d-%d.json", iMap, iReduce)
}

func makeFinalFileName(iReduce int) string {
	return fmt.Sprintf("mr-out-%d", iReduce)
}

func readBucket(fileName string) ([]KeyValue, error) {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	var bucket []KeyValue
	err = json.Unmarshal(bytes, &bucket)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func writeBucket(fileName string, bucket []KeyValue) error {
	fileContent, err := json.Marshal(bucket)
	if err != nil {
		return err
	}
	return atomicWriteFile(fileName, fileContent)
}

func atomicWriteFile(fileName string, fileContent []byte) error {
	tempFile, err := os.CreateTemp("", "")
	defer func() {
		_ = tempFile.Close()
	}()
	if err != nil {
		return err
	}

	_, err = tempFile.Write(fileContent)
	if err != nil {
		return err
	}
	_ = tempFile.Close()

	return os.Rename(tempFile.Name(), fileName)
}

// call send an RPC request to the coordinator, wait for the response.
func call(rpcName string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	defer func() {
		_ = c.Close()
	}()
	if err != nil {
		return err
	}

	return c.Call(rpcName, args, reply)
}
