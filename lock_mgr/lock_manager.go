package lock_mgr

import (
	"errors"
	"sync"
)

var Deadlock = errors.New("Deadlock detected!")

type lockManager struct {
	mu                    *sync.Mutex
	lockIDToLock          map[string]*lock
	clientToHeldLockIDs   map[string]map[string]struct{}
	clientToQueuedLockIDs map[string]map[string]struct{}
	clientKillChan        chan string
}

func NewLockManager() *lockManager {
	lm := &lockManager{
		mu:                    &sync.Mutex{},
		lockIDToLock:          make(map[string]*lock),
		clientToHeldLockIDs:   make(map[string]map[string]struct{}),
		clientToQueuedLockIDs: make(map[string]map[string]struct{}),
		clientKillChan:        make(chan string),
	}
	go lm.startDeadlockDetector()
	return lm
}

func markLockIDForClient(
	m map[string]map[string]struct{},
	clientID string,
	lockID string,
) {
	if _, ok := m[clientID]; !ok {
		m[clientID] = make(map[string]struct{})
	}
	m[clientID][lockID] = struct{}{}
}

func unmarkLockIDForClient(
	m map[string]map[string]struct{},
	clientID string,
	lockID string,
) {
	delete(m[clientID], lockID)
}

func (lm *lockManager) getOrCreateLock(lockID string) *lock {
	if _, ok := lm.lockIDToLock[lockID]; !ok {
		lm.lockIDToLock[lockID] = &lock{
			lockID: lockID,
		}
	}
	return lm.lockIDToLock[lockID]
}

func (lm *lockManager) Acquire(
	clientID string,
	lockID string,
	exclusive bool,
) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	markLockIDForClient(lm.clientToQueuedLockIDs, clientID, lockID)
	l := lm.getOrCreateLock(lockID)
	err := l.acquire(newRequest(clientID, exclusive, lm.mu))
	if err != nil {
		return err
	}
	unmarkLockIDForClient(lm.clientToQueuedLockIDs, clientID, lockID)
	markLockIDForClient(lm.clientToHeldLockIDs, clientID, lockID)
	return nil
}

func (lm *lockManager) ReleaseAll(clientID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for lock := range lm.clientToHeldLockIDs[clientID] {
		lm.lockIDToLock[lock].release(clientID)
	}
	delete(lm.clientToHeldLockIDs, clientID)
}
