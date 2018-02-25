package lock_mgr

import (
	"errors"
	"sync"
)

var Deadlock = errors.New("Deadlock detected!")

type lockManager struct {
	mu              *sync.Mutex
	lockIDToLock    map[string]*lock
	clientToLockIDs map[string]map[string]struct{}
	clientKillChan  chan string
}

func NewLockManager() *lockManager {
	lm := &lockManager{
		mu:              &sync.Mutex{},
		lockIDToLock:    make(map[string]*lock),
		clientToLockIDs: make(map[string]map[string]struct{}),
		clientKillChan:  make(chan string),
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

	if _, ok := lm.lockIDToLock[lockID]; !ok {
		lm.lockIDToLock[lockID] = &lock{
			lockID: lockID,
		}
	}
	err := lm.lockIDToLock[lockID].acquire(&request{
		clientID:  clientID,
		exclusive: exclusive,
		cond:      sync.NewCond(lm.mu),
	})
	if err != nil {
		return err
	}
	if _, ok := lm.clientToLockIDs[clientID]; !ok {
		lm.clientToLockIDs[clientID] = make(map[string]struct{})
	}
	lm.clientToLockIDs[clientID][lockID] = struct{}{}
	return nil
}

func (lm *lockManager) ReleaseAll(clientID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for lock := range lm.clientToLockIDs[clientID] {
		lm.lockIDToLock[lock].release(clientID)
	}
	delete(lm.clientToLockIDs, clientID)
}
