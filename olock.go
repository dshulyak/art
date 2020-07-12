package art

import (
	"runtime"
	"sync/atomic"
)

// olock is a implemention of Optimistic Lock.
// As descibed in https://15721.courses.cs.cmu.edu/spring2017/papers/08-oltpindexes2/leis-damon2016.pd// Appendix A: Implementation of Optimistic Locks
//
// The two least significant bits indicate if the node is obsolete or if the node is locked,
// respectively.  The remaining bits store the update counte
//
// Zero value is unlocked.
type olock uint64

// RLock waits for node to be unlocked and returns current version, possibly obsolete.
// If version is obsolete user must discard used object and restart execution.
// Read lock is a current value of uint64, if this value gets outdated at the time of RUnlock
// - read will need to be restarted.
func (ol *olock) RLock() (uint64, bool) {
	version := ol.waitUnlocked()
	return version, isObsolete(version)
}

// RUnlock compares read lock with current value of the olock, in case if
// value got changed - RUnlock will notify caller that operation needs to be restarted.
func (ol *olock) RUnlock(version uint64, locked *olock) bool {
	if atomic.LoadUint64((*uint64)(ol)) != version {
		if locked != nil {
			locked.Unlock()
		}
		return true
	}
	return false
}

// Upgrade current lock to write lock, in case of failure to update locked lock will be unlocked.
func (ol *olock) Upgrade(version uint64, locked *olock) bool {
	if !atomic.CompareAndSwapUint64((*uint64)(ol), version, setLockedBit(version)) {
		if locked != nil {
			locked.Unlock()
		}
		return true
	}
	return false
}

// Check returns true if version has changed.
func (ol *olock) Check(version uint64) bool {
	return !(atomic.LoadUint64((*uint64)(ol)) == version)
}

func (ol *olock) Lock() {
	var (
		version uint64
		ok      = true
	)
	for ok {
		version, ok = ol.RLock()
		if ok {
			continue
		}
		ok = ol.Upgrade(version, nil)
	}
}

func (ol *olock) Unlock() {
	atomic.AddUint64((*uint64)(ol), 2)
}

func (ol *olock) UnlockObsolete() {
	atomic.AddUint64((*uint64)(ol), 3)
}

func (ol *olock) waitUnlocked() uint64 {
	for {
		version := atomic.LoadUint64((*uint64)(ol))
		if version&2 != 2 {
			return version
		}
		runtime.Gosched()
	}
}

func isObsolete(version uint64) bool {
	return (version & 1) == 1
}

func setLockedBit(version uint64) uint64 {
	return version + 2
}
