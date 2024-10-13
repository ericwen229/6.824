package util

import (
	"fmt"
)

type Countdown struct {
	remainingMs int64
}

func NewCountdown(totalMs int64) *Countdown {
	return &Countdown{
		remainingMs: totalMs,
	}
}

func (c *Countdown) Tick(elapsedMs int64) bool {
	if elapsedMs < 0 {
		panic(fmt.Errorf("illegal tick elapsedMs: %v", elapsedMs))
	}

	if c.remainingMs <= 0 {
		return true
	}
	c.remainingMs -= elapsedMs
	return c.remainingMs <= 0
}
