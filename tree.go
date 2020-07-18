package art

import (
	"bytes"
)

type ValueType interface{}

type Tree struct {
	lock olock
	root node
}

func (t *Tree) Insert(key []byte, value ValueType) {
	for {
		version, restart := t.lock.RLock()
		l := &leaf{key: key, value: value}
		if t.root == nil {
			if t.lock.Upgrade(version, nil) {
				continue
			}
			t.root = l
			t.lock.Unlock()
			return
		}
		if t.root.isLeaf() {
			if t.lock.Upgrade(version, nil) {
				continue
			}
			t.root, _ = t.root.insert(l, 0, &t.lock, version)
			t.lock.Unlock()
			return
		}
		root, restart := t.root.insert(l, 0, &t.lock, version)
		if restart {
			continue
		}
		t.root = root
		return
	}
	panic("unreachable")
}

func (t *Tree) Get(key []byte) (ValueType, bool) {
	for {
		version, _ := t.lock.RLock()
		root := t.root
		if t.lock.RUnlock(version, nil) {
			continue
		}
		if root == nil {
			return nil, false
		}
		val, found, restart := root.get(key, 0, &t.lock, version)
		if restart {
			continue
		}
		return val, found
	}
	panic("unreachable")
}

func (t *Tree) Delete(key []byte) {
	for {
		version, _ := t.lock.RLock()
		l, isLeaf := t.root.(*leaf)
		if isLeaf && l.cmp(key) {
			if t.lock.Upgrade(version, nil) {
				continue
			}
			t.root = nil
			t.lock.Unlock()
			return
		} else if isLeaf {
			if t.lock.RUnlock(version, nil) {
				continue
			}
			return
		}

		if t.root.del(key, 0, &t.lock, version, func(rn node) {
			t.root = rn
		}) {
			continue
		}
		return
	}
	panic("unreachable")
}

func (t *Tree) Empty() bool {
	return t.root == nil
}

// testView returns tree structure in the format used for tests.
// Must preserve:
// - depth
// - prefix of the inner nodes
// - keys of the innner nodes
// - (optionally) keys of the leafs
func (t *Tree) testView() string {
	if t.root == nil {
		return ""
	}

	const (
		padding = "."
		nl      = "\n"
	)

	var (
		b       bytes.Buffer
		newLine bool
	)
	_ = t.root.walk(func(n node, depth int) bool {
		if newLine {
			_, _ = b.WriteString(nl)
		} else {
			newLine = true
		}
		for i := 0; i < depth; i++ {
			_, _ = b.WriteString(padding)
		}
		_, _ = b.WriteString(n.String())
		return true
	}, 0)
	return b.String()
}
