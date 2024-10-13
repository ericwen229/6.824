package raft

import (
	"fmt"
	"os"
)

var (
	logSwitch         = os.Getenv("DEBUG") != ""
	categorySwitchMap = map[string]bool{
		categoryState: true,
	}
)

const (
	categoryState = "state"
)

func (rf *Raft) logState(format string, args ...interface{}) {
	debugLog(categoryState, rf.me, format, args...)
}

func debugLog(category string, id int, format string, args ...interface{}) {
	if !logSwitch {
		return
	}

	if !categorySwitchMap[category] {
		return
	}

	fmt.Printf("[%s] [%d] %s\n", category, id, fmt.Sprintf(format, args...))
}
