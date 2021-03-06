package lock_mgr

import (
	"time"

	. "gopkg.in/check.v1"
)

type LockManagerSuite struct{}

var _ = Suite(&LockManagerSuite{})

const (
	testLockTimeout              = 250 * time.Millisecond
	testDeadlockDetectionTimeout = 5 * time.Second
)

func assertLockBehavior(
	c *C,
	lm *lockManager,
	clientID string,
	lockID string,
	exclusive bool,
	blockingExpected bool,
) {
	done := make(chan struct{}, 1)
	go func() {
		lm.Acquire(clientID, lockID, exclusive)
		close(done)
	}()
	var errFmt string
	select {
	case <-done:
		if blockingExpected {
			errFmt = "Client %v acquired %v (exclusive = %v) within %v"
		} else {
			return
		}
	case <-time.After(testLockTimeout):
		if !blockingExpected {
			errFmt = "Client %v failed to acquire %v (exclusive = %v) within %v"
		} else {
			return
		}
	}
	c.Errorf(errFmt, clientID, lockID, exclusive, testLockTimeout)
}

func (s *LockManagerSuite) TestLockManager(c *C) {
	lm := NewLockManager()

	// Acquiring shared locks should not block.
	assertLockBehavior(c, lm, "c1", "l1", false, false)
	assertLockBehavior(c, lm, "c2", "l1", false, false)
	assertLockBehavior(c, lm, "c3", "l1", false, false)

	// Trying to acquire an exclusive lock should block.
	assertLockBehavior(c, lm, "c4", "l1", true, true)

	// Trying to acquire another shared lock should block (waiting in line).
	assertLockBehavior(c, lm, "c5", "l1", false, true)

	lm.ReleaseAll("c1")
	lm.ReleaseAll("c2")
	lm.ReleaseAll("c3")

	// Wait for c4 to get the lock.
	time.Sleep(testLockTimeout)
	c.Assert(len(lm.lockIDToLock["l1"].holders), Equals, 1)
	c.Assert(lm.lockIDToLock["l1"].holders[0].clientID, Equals, "c4")

	// Locks are re-entrant (thus this should not block).
	assertLockBehavior(c, lm, "c4", "l1", true, false)

	// Exclusive lock is already held, so nothing need to be done to "acquire"
	// the shared lock.
	assertLockBehavior(c, lm, "c4", "l1", false, false)

	lm.ReleaseAll("c4")

	// Wait for c5 to get the lock.
	time.Sleep(testLockTimeout)
	c.Assert(len(lm.lockIDToLock["l1"].holders), Equals, 1)
	c.Assert(lm.lockIDToLock["l1"].holders[0].clientID, Equals, "c5")

	lm.ReleaseAll("c5")

	// Make sure there's nothing left.
	c.Assert(len(lm.lockIDToLock["l1"].holders), Equals, 0)
	c.Assert(len(lm.lockIDToLock["l1"].pending), Equals, 0)
}

func (s *LockManagerSuite) TestDeadlockDetector(c *C) {
	errChan := make(chan error, 3)
	lm := NewLockManager()
	lm.Acquire("c1", "l1", true)
	lm.Acquire("c2", "l2", true)
	lm.Acquire("c3", "l3", true)
	go func() {
		err := lm.Acquire("c1", "l2", true)
		if err != nil {
			errChan <- err
		}
	}()
	go func() {
		err := lm.Acquire("c2", "l3", true)
		if err != nil {
			errChan <- err
		}
	}()
	go func() {
		err := lm.Acquire("c3", "l1", true)
		if err != nil {
			errChan <- err
		}
	}()
	select {
	case <-errChan:
		// Deadlock successfully detected.
	case <-time.After(testDeadlockDetectionTimeout):
		c.Errorf(
			"Deadlock between c1, c2, c3 should have been detected after %v",
			testDeadlockDetectionTimeout)
	}
}

func (s *LockManagerSuite) TestLockUpgrade(c *C) {
	lm := NewLockManager()

	// Lock upgrade should succeed in this case.
	assertLockBehavior(c, lm, "c1", "l1", false, false)
	assertLockBehavior(c, lm, "c1", "l1", true, false)
	lm.ReleaseAll("c1")

	// Lock upgrade will block if another client holds the shared lock.
	assertLockBehavior(c, lm, "c1", "l1", false, false)
	assertLockBehavior(c, lm, "c2", "l1", false, false)
	assertLockBehavior(c, lm, "c1", "l1", true, true)
	lm.ReleaseAll("c2")
	time.Sleep(testLockTimeout)
	lm.ReleaseAll("c1")
	time.Sleep(testLockTimeout)

	// Lock upgrade will block if another client is in line.
	assertLockBehavior(c, lm, "c1", "l1", false, false)
	assertLockBehavior(c, lm, "c2", "l1", true, true)
	assertLockBehavior(c, lm, "c1", "l1", true, true)
}
