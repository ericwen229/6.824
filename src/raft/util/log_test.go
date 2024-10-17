package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendAndQuery(t *testing.T) {
	logs := NewEntries()
	assert.Equal(t, 0, logs.Len())

	logs.Append(&LogEntry{1, 1})
	logs.Append(&LogEntry{2, 2})
	logs.Append(&LogEntry{3, 3})
	assert.Equal(t, 3, logs.Len())
	assert.Equal(t, 3, logs.LastIndex())
	assert.True(t, logs.Match(0, 0))
	assert.True(t, logs.Match(0, 2147483647))
	assert.True(t, logs.Match(1, 1))
	assert.True(t, logs.Match(2, 2))
	assert.True(t, logs.Match(3, 3))
	assert.False(t, logs.Match(2, 1))
	assert.False(t, logs.Match(2, 3))
	assert.False(t, logs.Match(4, 4))
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
	assert.Equal(t, 2, logs.Len())
	assert.Equal(t, 2, logs.LastIndex())
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
	assert.Equal(t, 5, logs.Len())
	assert.Equal(t, 3, logs.get(3).Command.(int))

	logs.setOrAppend(6, &LogEntry{6, 6})
	assert.Equal(t, 6, logs.Len())
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
	assert.Equal(t, 3, logs.Len())
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
	entries := logs.StartingFrom(3)
	assert.Equal(t, 3, len(entries))
	assert.Equal(t, 3, entries[0].Term)
	assert.Equal(t, 4, entries[1].Term)
	assert.Equal(t, 5, entries[2].Term)
}
