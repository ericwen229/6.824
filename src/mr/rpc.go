package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// 请求任务
//

const (
	SUCCESS = 0
	PENDING = 1
	DONE    = 2
)

type RequestTaskRequest struct {}

type RequestTaskResponse struct {
	Code int
	Task MapReduceTask
}

//
// 完成任务
//

type CompleteTaskRequest struct {
	TaskId int
}

type CompleteTaskResponse struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
