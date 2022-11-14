package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	taskNum            int
	mapTaskNum         int
	reduceTaskNum      int
	completeMapTaskNum int
	completeTaskNum    int
	taskList           []*Task
	taskBeginTime      []int64
	isDone             atomic.Value
	mutex              sync.Mutex
}

type Task struct {
	ID         int
	Type       TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type TaskType int8

const (
	TaskMap    TaskType = 1
	TaskReduce TaskType = 2
)

type MapTask struct {
	File    string
	IMap    int
	NReduce int
}
type ReduceTask struct {
	NMap    int
	IReduce int
}

func (c *Coordinator) RequestTask(req *RequestTaskReq, resp *RequestTaskResp) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if req.CompleteTaskId >= 0 {
		c.completeTask(req.CompleteTaskId)
	}

	if c.Done() {
		resp.IsDone = true
		resp.Task = nil
		return nil
	}

	resp.IsDone = false
	resp.Task = c.selectTask()
	return nil
}

func (c *Coordinator) completeTask(id int) {
	if c.taskBeginTime[id] < 0 {
		return
	} else if c.taskBeginTime[id] == 0 {
		panic(fmt.Sprintf("task #%d completed before started", id))
	}

	// complete task
	c.taskBeginTime[id] = -time.Now().Unix()

	// update stats
	c.completeTaskNum++
	if id < c.mapTaskNum {
		c.completeMapTaskNum++
	}

	// update state
	if c.completeTaskNum == c.taskNum {
		c.isDone.Store(true)
	}
}

func (c *Coordinator) selectTask() *Task {
	start := 0
	end := c.mapTaskNum
	if c.completeMapTaskNum == c.mapTaskNum {
		// select reduce task
		start = c.mapTaskNum
		end = c.taskNum
	}

	for i := start; i < end; i++ {
		if c.taskBeginTime[i] == 0 {
			// task state: initial
			c.taskBeginTime[i] = time.Now().Unix()
			return c.taskList[i]
		} else if c.taskBeginTime[i] < 0 {
			// task state: completed
			continue
		} else if time.Now().Unix()-c.taskBeginTime[i] > 10 {
			// task state: started && timeout
			c.taskBeginTime[i] = time.Now().Unix()
			return c.taskList[i]
		}
	}
	return nil
}

// server starts the server thread
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	_ = os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		_ = http.Serve(l, nil)
	}()
}

// Done is called periodically to find out
// if the entire job has finished
func (c *Coordinator) Done() bool {
	return c.isDone.Load().(bool)
}

// MakeCoordinator creates a Coordinator
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTaskNum := len(files)
	reduceTaskNum := nReduce
	taskNum := mapTaskNum + reduceTaskNum

	var mapTaskList []*Task
	for iMap, file := range files {
		mapTaskList = append(mapTaskList, &Task{
			ID:   0, // set later
			Type: TaskMap,
			MapTask: &MapTask{
				File:    file,
				IMap:    iMap,
				NReduce: reduceTaskNum,
			},
			ReduceTask: nil,
		})
	}

	var reduceTaskList []*Task
	for iReduce := 0; iReduce < reduceTaskNum; iReduce++ {
		reduceTaskList = append(reduceTaskList, &Task{
			ID:      0, // set later
			Type:    TaskReduce,
			MapTask: nil,
			ReduceTask: &ReduceTask{
				NMap:    mapTaskNum,
				IReduce: iReduce,
			},
		})
	}

	taskList := mapTaskList
	taskList = append(taskList, reduceTaskList...)
	for i, task := range taskList {
		task.ID = i
	}

	c := Coordinator{
		taskNum:       taskNum,
		mapTaskNum:    mapTaskNum,
		reduceTaskNum: reduceTaskNum,
		taskList:      taskList,
		taskBeginTime: make([]int64, taskNum),
	}
	c.isDone.Store(false)
	c.server()
	return &c
}
