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
type iterator struct {
	tree *Tree

	stack  *checkpoint
	closed bool

	cursor, terminate []byte
	reverse           bool

	key   []byte
	value ValueType
}

func (i *iterator) Reverse() *iterator {
	i.cursor, i.terminate = i.terminate, i.cursor
	i.reverse = true
	return i
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
	return i.iterate()
}

func (i *iterator) Value() ValueType {
	return i.value
}

func (i *iterator) Key() []byte {
	return i.key
}

func (i *iterator) inRange(key []byte) bool {
	if !i.reverse {
		return bytes.Compare(key, i.cursor) > 0 && (len(i.terminate) == 0 || bytes.Compare(key, i.terminate) <= 0)
	}
	return (bytes.Compare(key, i.cursor) < 0 || len(i.cursor) == 0) && (len(i.terminate) == 0 || bytes.Compare(key, i.terminate) >= 0)
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
			if i.inRange(l.key) {
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

func (i *iterator) next(n *inner, pointer *byte) (byte, node) {
	if !i.reverse {
		return n.node.next(pointer)
	}
	return n.node.prev(pointer)
}

func (i *iterator) iterate() bool {
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

		pointer, child := i.next(tail.node, tail.pointer)

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
			if i.inRange(l.key) {
				i.key = l.key
				i.value = l.value
				i.cursor = l.key
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
