package dag

import (
	"sync"
)

type WaitGroup struct {
	sync.Mutex
	counter int
	cond    *sync.Cond
}

func NewWaitGroup() *WaitGroup {
	awg := &WaitGroup{}
	awg.cond = sync.NewCond(&awg.Mutex)
	return awg
}

// Add increments the counter for an async task
func (awg *WaitGroup) Add(delta int) {
	awg.Lock()
	awg.counter += delta
	awg.Unlock()
}

// Reset sets the counter to zero and notifies waiting goroutines
func (awg *WaitGroup) Reset() {
	awg.Lock()
	awg.counter = 0
	awg.cond.Broadcast() // Notify any waiting goroutines that we're done
	awg.Unlock()
}

// Done decrements the counter when a task is completed
func (awg *WaitGroup) Done() {
	awg.Lock()
	awg.counter--
	if awg.counter == 0 {
		awg.cond.Broadcast() // Notify all waiting goroutines
	}
	awg.Unlock()
}

// Wait blocks until the counter is zero
func (awg *WaitGroup) Wait() {
	awg.Lock()
	for awg.counter > 0 {
		awg.cond.Wait() // Wait for notification
	}
	awg.Unlock()
}
