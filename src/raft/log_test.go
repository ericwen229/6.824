package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendAndQuery(t *testing.T) {
	logs := newEntries()
	assert.Equal(t, 0, logs.len())

	logs.append(&LogEntry{1, 1})
	logs.append(&LogEntry{2, 2})
	logs.append(&LogEntry{3, 3})
	assert.Equal(t, 3, logs.len())
	assert.Equal(t, 3, logs.lastIndex())
	assert.True(t, logs.match(0, 0))
	assert.True(t, logs.match(0, 2147483647))
	assert.True(t, logs.match(1, 1))
	assert.True(t, logs.match(2, 2))
	assert.True(t, logs.match(3, 3))
	assert.False(t, logs.match(2, 1))
	assert.False(t, logs.match(2, 3))
	assert.False(t, logs.match(4, 4))
	assert.Equal(t, 1, logs.get(1).Term)
	assert.Equal(t, 2, logs.get(2).Term)
	assert.Equal(t, 3, logs.get(3).Term)
}

func TestTruncate(t *testing.T) {
	logs := &LogEntries{log: []*LogEntry{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}}
	logs.truncateFrom(3)
	assert.Equal(t, 2, logs.len())
	assert.Equal(t, 2, logs.lastIndex())
	assert.Equal(t, 1, logs.get(1).Term)
	assert.Equal(t, 2, logs.get(2).Term)
}

func TestSetOrAppendNoTruncate(t *testing.T) {
	logs := &LogEntries{log: []*LogEntry{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}}
	logs.setOrAppend(3, &LogEntry{3, 0}) // value has no effect
	assert.Equal(t, 5, logs.len())
	assert.Equal(t, 3, logs.get(3).Command.(int))

	logs.setOrAppend(6, &LogEntry{6, 6})
	assert.Equal(t, 6, logs.len())
	assert.Equal(t, 6, logs.get(6).Command.(int))
}

func TestSetOrAppendTruncate(t *testing.T) {
	logs := &LogEntries{log: []*LogEntry{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}}
	logs.setOrAppend(3, &LogEntry{4, 0})
	assert.Equal(t, 3, logs.len())
	assert.Equal(t, 4, logs.get(3).Term)
	assert.Equal(t, 0, logs.get(3).Command.(int))
}

func TestStartingFrom(t *testing.T) {
	logs := &LogEntries{log: []*LogEntry{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
	}}
	entries := logs.getEntriesStartingFrom(3)
	assert.Equal(t, 3, len(entries))
	assert.Equal(t, 3, entries[0].Term)
	assert.Equal(t, 4, entries[1].Term)
	assert.Equal(t, 5, entries[2].Term)
}
