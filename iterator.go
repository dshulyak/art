package art

import (
	"bytes"
)

type checkpoint struct {
	node          *inner
	parentLock    *olock
	parentVersion uint64
	pointer       *byte

	prev *checkpoint
}

// iterator will scan the tree in lexicographic order.
// not concurrently safe
type iterator struct {
	tree *Tree

	stack  *checkpoint
	closed bool

	current    []byte
	start, end []byte

	key   []byte
	value ValueType
}

// Next will iterate over all leaf nodes inbetween specified prefixes
func (i *iterator) Next() bool {
	if i.closed {
		return false
	}
	if i.stack == nil {
		// initialize iterator
		if exit, next := i.init(); exit {
			return next
		}
	}
	return i.next()
}

func (i *iterator) Value() ValueType {
	return i.value
}

func (i *iterator) Key() []byte {
	return i.key
}

// updateCurrent to the key + 1 byte
// if last byte is 0xff - next byte will be set to 0xff
// if last byte is less than 0xff - last byte will be incremented
func (i *iterator) updateCurrent(key []byte) {
	lth := len(key)
	ff := false
	if key[lth-1] == 0xff {
		lth++
		ff = true
	}
	if i.current == nil || len(i.current) < len(key) {
		i.current = make([]byte, lth)
		copy(i.current, key)
	} else if len(i.current) > len(key) {
		i.current = i.current[:lth]
		copy(i.current, key)
	}
	if !ff {
		i.current[len(i.current)-1]++
	}
}

func (i *iterator) init() (bool, bool) {
	for {
		version, _ := i.tree.lock.RLock()

		root := i.tree.root
		if root == nil {
			if i.tree.lock.RUnlock(version, nil) {
				continue
			}
			i.closed = true
			return true, false
		}
		l, isLeaf := root.(*leaf)
		if isLeaf {
			if i.tree.lock.RUnlock(version, nil) {
				continue
			}

			i.closed = true
			cmp := bytes.Compare(l.key, i.current)
			if cmp >= 0 && (i.end == nil || bytes.Compare(l.key, i.end) <= 0) {
				i.key = l.key
				i.value = l.value
				return true, true
			}
			return true, false
		}
		i.stack = &checkpoint{
			node:          root.(*inner),
			parentLock:    &i.tree.lock,
			parentVersion: version,
		}
		return false, false
	}
}

func (i *iterator) next() bool {
	for i.stack != nil {
		more, restart := i.tryAdvance()
		if more {
			return more
		} else if restart {
			i.stack = i.stack.prev

			if i.stack == nil {
				// checkpoint is root
				i.stack = nil
				if exit, next := i.init(); exit {
					return next
				}
			}
		}
	}
	i.closed = true
	return false
}

func (i *iterator) tryAdvance() (bool, bool) {
	for {
		tail := i.stack

		version, obsolete := tail.node.lock.RLock()
		if obsolete || tail.parentLock.Check(tail.parentVersion) {
			_ = tail.parentLock.RUnlock(version, nil)
			return false, true
		}

		pointer, child := tail.node.node.next(tail.pointer)

		if child == nil {
			if tail.node.lock.RUnlock(version, nil) {
				continue
			}
			_ = tail.parentLock.RUnlock(version, nil)
			// inner node is exhausted, move one level up the stack
			i.stack = tail.prev
			return false, false
		}
		// advance pointer
		tail.pointer = &pointer

		l, isLeaf := child.(*leaf)
		if isLeaf {
			cmp := bytes.Compare(l.key, i.current)
			if cmp >= 0 && (i.end == nil || bytes.Compare(l.key, i.end) <= 0) {
				i.key = l.key
				i.value = l.value
				i.updateCurrent(i.key)
				return true, false
			}
			return false, false
		}
		i.stack = &checkpoint{
			node:          child.(*inner),
			prev:          tail,
			parentLock:    &tail.node.lock,
			parentVersion: version,
		}
		return false, false
	}
}
