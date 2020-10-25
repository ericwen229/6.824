package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	mutex              sync.Mutex
	pendingMapTasks    map[int]*MapTask
	runningMapTasks    map[int]*MapTask
	pendingReduceTasks map[int]*ReduceTask
	runningReduceTasks map[int]*ReduceTask
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.pendingMapTasks) == 0 &&
		len(m.runningMapTasks) == 0 &&
		len(m.pendingReduceTasks) == 0 &&
		len(m.runningReduceTasks) == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	intermediateFilePathTmpl := "mr-%d-%d"
	outputFilePathTmpl := "mr-out-%d"

	m := Master{
		pendingMapTasks:    map[int]*MapTask{},
		runningMapTasks:    map[int]*MapTask{},
		pendingReduceTasks: map[int]*ReduceTask{},
		runningReduceTasks: map[int]*ReduceTask{},
	}

	for iMap, file := range files {
		m.pendingMapTasks[iMap] = &MapTask{
			id:                 iMap,
			inputFilePath:      file,
			nReduce:            nReduce,
			outputFilePathTmpl: intermediateFilePathTmpl,
		}
	}

	for iReduce := 0; iReduce < nReduce; iReduce++ {
		var inputFilePaths []string
		for iMap := 0; iMap < len(files); iMap++ {
			inputFilePaths = append(inputFilePaths, fmt.Sprintf(intermediateFilePathTmpl, iMap, iReduce))
		}
		m.pendingReduceTasks[iReduce] = &ReduceTask{
			id:             iReduce,
			inputFilePaths: inputFilePaths,
			outputFilePath: fmt.Sprintf(outputFilePathTmpl, iReduce),
		}
	}

	m.server()
	return &m
}
