package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging

type ServerStatus int
type logTopic string

var debugVerbosity int
var debugStart time.Time

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

const (
	Follower ServerStatus = iota + 1
	Candidate
	Leader
)

const (
	dApply   logTopic = "APPL"
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func getVerbosity() int {
	//v := os.Getenv("VERBOSE")
	//level := 0
	//if v != "" {
	//	var err error
	//	level, err = strconv.Atoi(v)
	//	if err != nil {
	//		log.Fatalf("Invalid verbosity %v", v)
	//	}
	//}
	return 1
}

func Debug(topic logTopic, format string, a ...interface{}) {
	//println("debugVerbosity %d", debugVerbosity)
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
