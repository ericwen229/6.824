package raft

import (
	"fmt"
	"os"
	"strings"
	"time"
)

var logSwitch bool
var logStart time.Time

func init() {
	logSwitch = isLogSwitchOn()
	logStart = time.Now()
}

func isLogSwitchOn() bool {
	return os.Getenv("DEBUG") != ""
}

func (rf *Raft) log(fmtStr string, args ...interface{}) {
	if logSwitch {
		milli := time.Since(logStart).Milliseconds()
		prefix := fmt.Sprintf("%06d S%d ", milli, rf.me)
		fmtStr := prefix + fmtStr
		fmt.Printf(fmtStr+"\n", args...)
	}
}

func formatEntries(entries []*LogEntry) string {
	var builder strings.Builder
	builder.WriteString("[")
	for i, entry := range entries {
		if i != 0 {
			builder.WriteString(",")
		}
		builder.WriteString(fmt.Sprintf("%d:", i+1))
		builder.WriteString(formatEntry(entry))
	}
	builder.WriteString("]")
	return builder.String()
}

func formatEntry(entry *LogEntry) string {
	return fmt.Sprintf("{%d:%v}", entry.Term, entry.Command)
}
