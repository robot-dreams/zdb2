package lock_mgr

import (
	"sync"
)

type lockManager struct {
	mu              *sync.Mutex
	lockIDToLock    map[string]*lock
	clientToLockIDs map[string][]string
	clientKillChan  chan string
}

func NewLockManager() *lockManager {
	lm := &lockManager{
		mu:              &sync.Mutex{},
		lockIDToLock:    make(map[string]*lock),
		clientToLockIDs: make(map[string][]string),
		clientKillChan:  make(chan string),
	}
	go lm.startDeadlockDetector()
	return lm
}

func (lm *lockManager) Acquire(clientID string, lockID string, exclusive bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, ok := lm.lockIDToLock[lockID]; !ok {
		lm.lockIDToLock[lockID] = &lock{
			lockID: lockID,
		}
	}
	lm.lockIDToLock[lockID].acquire(request{
		clientID:  clientID,
		exclusive: exclusive,
		cond:      sync.NewCond(lm.mu),
	})
	lm.clientToLockIDs[clientID] = append(lm.clientToLockIDs[clientID], lockID)
}

func (lm *lockManager) ReleaseAll(clientID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for _, lock := range lm.clientToLockIDs[clientID] {
		lm.lockIDToLock[lock].release(clientID)
	}
}
