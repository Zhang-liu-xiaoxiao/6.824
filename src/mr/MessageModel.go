package mr

type RequestEnum int
type TaskEnum int

const (
	AskTask RequestEnum = iota + 1
	FinishTask
)

const (
	Map TaskEnum = iota + 1
	Reduce
)

type WorkerRequest struct {
	RequestType  RequestEnum
	TaskType     TaskEnum
	TaskSequence int
}

type MasterResponse struct {
	TaskSequence int
	FileName     []string
	FinishMark   bool
	TaskType     TaskEnum
	NReduce      int
	Bubble       bool
}
