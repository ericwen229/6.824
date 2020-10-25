package mr

import "encoding/gob"

func init() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
}

type MapReduceTask interface {
	Execute(mapf func(string, string) []KeyValue, reducef func(string, []string) string) error
}

type MapTask struct {
	IMap               int
	InputFilePath      string
	NReduce            int
	OutputFilePathTmpl string
}

type ReduceTask struct {
	IReduce        int
	InputFilePaths []string
	OutputFilePath string
}

func (t *MapTask) Execute(mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	// TODO: 实现map worker逻辑
	return nil
}

	// TODO: 实现reduce worker逻辑
func (t *ReduceTask) Execute(mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	return nil
}