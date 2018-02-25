package lock_mgr

import (
	"sync"

	"github.com/dropbox/godropbox/errors"
)

var Deadlock = errors.New("Deadlock detected!")

type lockManager struct {
	mu                     *sync.Mutex
	lockIDToLock           map[string]*lock
	clientToHeldLockIDs    map[string]map[string]struct{}
	clientToPendingRequest map[string]*request
}

func NewLockManager() *lockManager {
	lm := &lockManager{
		mu:                     &sync.Mutex{},
		lockIDToLock:           make(map[string]*lock),
		clientToHeldLockIDs:    make(map[string]map[string]struct{}),
		clientToPendingRequest: make(map[string]*request),
	}
	go lm.startDeadlockDetector()
	return lm
}

func (lm *lockManager) Acquire(
	clientID string,
	lockID string,
	exclusive bool,
) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// A client can only have one pending request at a time.
	if r, ok := lm.clientToPendingRequest[clientID]; ok {
		return errors.Newf(
			"Client %v already has a pending request %+v",
			clientID,
			r)
	}

	l := lm.getOrCreateLock(lockID)
	r := newRequest(clientID, exclusive, lm.mu)
	lm.clientToPendingRequest[clientID] = r
	err := l.acquire(r)
	// We clear the pending request as soon as l.acquire returns, whether or not
	// the acquire was successful.
	delete(lm.clientToPendingRequest, clientID)
	if err != nil {
		return err
	}
	lm.markHeldLockID(clientID, lockID)
	return nil
}

func (lm *lockManager) getOrCreateLock(lockID string) *lock {
	if _, ok := lm.lockIDToLock[lockID]; !ok {
		lm.lockIDToLock[lockID] = &lock{
			lockID: lockID,
		}
	}
	return lm.lockIDToLock[lockID]
}

func (lm *lockManager) markHeldLockID(clientID string, lockID string) {
	if _, ok := lm.clientToHeldLockIDs[clientID]; !ok {
		lm.clientToHeldLockIDs[clientID] = make(map[string]struct{})
	}
	lm.clientToHeldLockIDs[clientID][lockID] = struct{}{}
}

func (lm *lockManager) ReleaseAll(clientID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for lock := range lm.clientToHeldLockIDs[clientID] {
		lm.lockIDToLock[lock].release(clientID)
	}
	delete(lm.clientToHeldLockIDs, clientID)
}
