package lock_mgr

import (
	"sync"

	"github.com/dropbox/godropbox/errors"
)

type request struct {
	clientID         string
	exclusive        bool
	cond             *sync.Cond
	deadlockDetected bool
}

func newRequest(
	clientID string,
	exclusive bool,
	mu *sync.Mutex,
) *request {
	return &request{
		clientID:         clientID,
		exclusive:        exclusive,
		cond:             sync.NewCond(mu),
		deadlockDetected: false,
	}
}

type lock struct {
	lockID string

	// Invariants:
	//     If len(holders) > 0, then every element has exclusive == false
	//     Each clientID appears at most once
	holders []*request

	// Invariant:
	//     Each clientID appears at most once
	pending []*request
}

func (l *lock) removeFromPending(clientID string) {
	j := 0
	for i := 0; i < len(l.pending); i++ {
		if l.pending[i].clientID != clientID {
			l.pending[j] = l.pending[i]
			j++
		}
	}
	l.pending = l.pending[:j]
}

// Precondition:
//     r.clientID holds the lock in shared mode
func (l *lock) upgrade(r *request) error {
	l.pending = append(l.pending, r)
	for l.pending[0] != r || len(l.holders) > 1 {
		r.cond.Wait()
		if r.deadlockDetected {
			l.removeFromPending(r.clientID)
			return Deadlock
		}
	}
	l.pending = l.pending[1:]
	l.holders[0].exclusive = true
	return nil
}

func (l *lock) canAcquire(exclusive bool) bool {
	if len(l.holders) == 0 {
		return true
	} else {
		return !exclusive && !l.holders[0].exclusive
	}
}

func (l *lock) acquire(r *request) error {
	for _, holder := range l.holders {
		if r.clientID == holder.clientID {
			if r.exclusive && !holder.exclusive {
				return l.upgrade(r)
			}
			return nil
		}
	}
	l.pending = append(l.pending, r)
	for l.pending[0] != r || !l.canAcquire(r.exclusive) {
		r.cond.Wait()
		if r.deadlockDetected {
			l.removeFromPending(r.clientID)
			return Deadlock
		}
	}
	l.pending = l.pending[1:]
	l.holders = append(l.holders, r)
	return nil
}

// Precondition:
//     0 <= i < len(holders)
func (l *lock) removeHolder(i int) {
	l.holders = append(l.holders[:i], l.holders[i+1:]...)
	if len(l.pending) == 0 {
		return
	} else if l.pending[0].exclusive {
		canAcquire := len(l.holders) == 0
		canUpgrade := len(l.holders) == 1 &&
			l.pending[0].clientID == l.holders[0].clientID
		if canAcquire || canUpgrade {
			l.pending[0].cond.Signal()
		}
	} else {
		if len(l.holders) > 0 {
			return
		}
		for i := 0; i < len(l.pending) && !l.pending[i].exclusive; i++ {
			l.pending[i].cond.Signal()
		}
	}
}

func (l *lock) release(clientID string) {
	for i, holder := range l.holders {
		if holder.clientID == clientID {
			l.removeHolder(i)
			return
		}
	}
	panic(errors.Newf(
		"lock %v was not held by client %v",
		l.lockID,
		clientID))
}
