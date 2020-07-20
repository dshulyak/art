// +build race

package art

import "sync"

// olock implements pessimistic locking, golang race detector won't be able
// to recognize correctness of the optimistic locking and will report races
// if tests are executed with --race flag
type olock struct {
	mu sync.Mutex
}

func (ol *olock) RLock() (uint64, bool) {
	ol.mu.Lock()
	return 0, false
}

// RUnlock compares read lock with current value of the olock, in case if
// value got changed - RUnlock will return true.
func (ol *olock) RUnlock(version uint64, locked *olock) bool {
	ol.mu.Unlock()
	return false
}

// Upgrade current lock to write lock, in case of failure to update locked lock will be unlocked.
func (ol *olock) Upgrade(version uint64, locked *olock) bool {
	return false
}

// Check returns true if version has changed.
func (ol *olock) Check(version uint64) bool {
	return false
}

func (ol *olock) Lock() {
	ol.mu.Lock()
}

func (ol *olock) Unlock() {
	ol.mu.Unlock()
}

func (ol *olock) UnlockObsolete() {
	ol.mu.Unlock()
}
