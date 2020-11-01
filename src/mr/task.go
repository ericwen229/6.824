package mr

type MapReduceTask struct {
	MTask *MapTask
	RTask *ReduceTask
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
