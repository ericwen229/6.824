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
	// 其实map任务和reduce任务可以分开调度
	// 用单个互斥锁管理实现较为简单
	mutex              sync.Mutex
	PendingMapTasks    map[int]*MapTask
	RunningMapTasks    map[int]*MapTask
	PendingReduceTasks map[int]*ReduceTask
	RunningReduceTasks map[int]*ReduceTask
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
	return len(m.PendingMapTasks) == 0 &&
		len(m.RunningMapTasks) == 0 &&
		len(m.PendingReduceTasks) == 0 &&
		len(m.RunningReduceTasks) == 0
}

//
// 请求任务
//
func (m *Master) RequestTask(req *RequestTaskRequest, resp *RequestTaskResponse) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.PendingMapTasks) > 0 {  // 分发map任务
	} else if len(m.RunningMapTasks) > 0 {  // 等待
	} else if len(m.PendingReduceTasks) > 0 {  // 分发reduce任务
	} else if len(m.RunningReduceTasks) > 0 {  // 等待
	} else {  // 所有任务已完成
	}
	return nil
}

//
// 完成任务
//
func (m *Master) CompleteTask(req *CompleteTaskRequest, resp *CompleteTaskResponse) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.PendingMapTasks, req.TaskId)
	delete(m.RunningMapTasks, req.TaskId)
	delete(m.PendingReduceTasks, req.TaskId)
	delete(m.RunningReduceTasks, req.TaskId)
	return nil
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
		PendingMapTasks:    map[int]*MapTask{},
		RunningMapTasks:    map[int]*MapTask{},
		PendingReduceTasks: map[int]*ReduceTask{},
		RunningReduceTasks: map[int]*ReduceTask{},
	}

	// 创建pending中的map任务
	taskId := 0
	for iMap, file := range files {
		m.PendingMapTasks[taskId] = &MapTask{
			IMap:               iMap,
			InputFilePath:      file,
			NReduce:            nReduce,
			OutputFilePathTmpl: intermediateFilePathTmpl,
		}
		taskId++
	}

	// 创建pending中的reduce任务
	for iReduce := 0; iReduce < nReduce; iReduce++ {
		var inputFilePaths []string
		for iMap := 0; iMap < len(files); iMap++ {  // 每个reduce任务都要接收来自每一个map任务的输出作为输入
			inputFilePaths = append(inputFilePaths, fmt.Sprintf(intermediateFilePathTmpl, iMap, iReduce))
		}
		m.PendingReduceTasks[taskId] = &ReduceTask{
			IReduce:        iReduce,
			InputFilePaths: inputFilePaths,
			OutputFilePath: fmt.Sprintf(outputFilePathTmpl, iReduce),
		}
		taskId++
	}

	// TODO: 打印调试

	m.server()
	return &m
}
