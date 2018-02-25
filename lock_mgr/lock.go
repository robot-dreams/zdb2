package lock_mgr

import (
	"sync"
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

func (l *lock) acquire(r *request) error {
	// If the client already holds this lock, then we either do nothing, or try
	// to upgrade from shared to xxclusive.
	for _, holder := range l.holders {
		if r.clientID == holder.clientID {
			if r.exclusive && !holder.exclusive {
				return l.upgrade(r)
			}
			return nil
		}
	}

	// The client doesn't hold this lock yet, so we actually try to acquire it.
	l.pending = append(l.pending, r)
	for l.pending[0] != r || !l.canAcquire(r.exclusive) {
		r.cond.Wait()
		if r.deadlockDetected {
			l.handleDeadlock(r.clientID)
			return Deadlock
		}
	}
	l.pending = l.pending[1:]
	l.holders = append(l.holders, r)
	return nil
}

// Precondition:
//     r.clientID holds the lock in shared mode
//     r.exclusive == true
func (l *lock) upgrade(r *request) error {
	l.pending = append(l.pending, r)
	for l.pending[0] != r || len(l.holders) > 1 {
		r.cond.Wait()
		if r.deadlockDetected {
			l.handleDeadlock(r.clientID)
			return Deadlock
		}
	}
	l.pending = l.pending[1:]
	l.holders[0].exclusive = true
	return nil
}

func (l *lock) handleDeadlock(clientID string) {
	removeClientRequests(&l.pending, clientID)
	l.signalPendingRequests()
}

func removeClientRequests(requests *[]*request, clientID string) {
	for i := 0; i < len(*requests); i++ {
		if (*requests)[i].clientID == clientID {
			(*requests) = append((*requests)[:i], (*requests)[i+1:]...)
		}
	}
}

func (l *lock) signalPendingRequests() {
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

// Returns whether a new client (that doesn't already hold the lock in any mode)
// can acquire the lock in the given mode.
func (l *lock) canAcquire(exclusive bool) bool {
	if len(l.holders) == 0 {
		return true
	} else {
		return !exclusive && !l.holders[0].exclusive
	}
}

func (l *lock) release(clientID string) {
	removeClientRequests(&l.holders, clientID)
	l.signalPendingRequests()
}
