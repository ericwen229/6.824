package raft

import (
	"fmt"
	"os"
	"time"
)

var (
	logSwitch         = os.Getenv("DEBUG") != ""
	categorySwitchMap = map[string]bool{
		categoryState:     true,
		categoryElection:  true,
		categoryReplicate: true,
		categoryTest:      true,
	}
)

const (
	categoryState     = "state"
	categoryElection  = "elect"
	categoryReplicate = "replicate"
	categoryTest      = "test"
)

func (rf *Raft) logState(format string, args ...interface{}) {
	debugLog(categoryState, rf.me, format, args...)
}

func (rf *Raft) logElection(format string, args ...interface{}) {
	debugLog(categoryElection, rf.me, format, args...)
}

func (rf *Raft) logReplicate(format string, args ...interface{}) {
	debugLog(categoryReplicate, rf.me, format, args...)
}

func logTest(format string, args ...interface{}) {
	debugLog(categoryTest, -1, format, args...)
}

func debugLog(category string, id int, format string, args ...interface{}) {
	if !logSwitch {
		return
	}

	if !categorySwitchMap[category] {
		return
	}

	if id >= 0 {
		fmt.Printf("%s [%s] [%d] %s\n", time.Now().Format("15:04:05.000"), category, id, fmt.Sprintf(format, args...))
	} else {
		fmt.Printf("%s [%s] %s\n", time.Now().Format("15:04:05.000"), category, fmt.Sprintf(format, args...))
	}
}
