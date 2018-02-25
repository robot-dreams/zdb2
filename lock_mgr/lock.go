package lock_mgr

import (
	"sync"

	"github.com/dropbox/godropbox/errors"
)

type request struct {
	clientID  string
	exclusive bool
	cond      *sync.Cond
}

type lock struct {
	lockID string

	// Invariants:
	//     If len(holders) > 0, then every element has exclusive == false
	//     Each clientID appears at most once
	holders []request

	// Invariant:
	//     Each clientID appears at most once
	queue []request
}

// Precondition:
//     r.clientID holds the lock in shared mode
func (l *lock) upgrade(r request) {
	l.queue = append(l.queue, r)
	for l.queue[0] != r || len(l.holders) > 1 {
		r.cond.Wait()
	}
	l.queue = l.queue[1:]
	l.holders[0].exclusive = true
}

func (l *lock) canAcquire(exclusive bool) bool {
	if len(l.holders) == 0 {
		return true
	} else {
		return !exclusive && !l.holders[0].exclusive
	}
}

func (l *lock) acquire(r request) {
	for _, holder := range l.holders {
		if r.clientID == holder.clientID {
			if r.exclusive && !holder.exclusive {
				l.upgrade(r)
			}
			return
		}
	}
	l.queue = append(l.queue, r)
	for l.queue[0] != r || !l.canAcquire(r.exclusive) {
		r.cond.Wait()
	}
	l.queue = l.queue[1:]
	l.holders = append(l.holders, r)
}

// Precondition:
//     0 <= i < len(holders)
func (l *lock) removeHolder(i int) {
	l.holders = append(l.holders[:i], l.holders[i+1:]...)
	if len(l.holders) > 0 {
		return
	}
	if len(l.queue) == 0 {
		return
	}
	if l.queue[0].exclusive {
		l.queue[0].cond.Signal()
	} else {
		for i := 0; i < len(l.queue) && !l.queue[i].exclusive; i++ {
			l.queue[i].cond.Signal()
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
