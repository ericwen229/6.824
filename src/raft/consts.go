package raft

// index
const zeroIndex = 0

// term
const nanTerm = -1
const zeroTerm = 0

// id
const votedForNoOne = -1

// role
type role string

const (
	follower  role = "follower"
	candidate role = "candidate"
	leader    role = "leader"
)
