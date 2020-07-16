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
	var (
		restart = true
		version uint64
	)
	for restart {
		version, restart = t.lock.RLock()
		if restart {
			continue
		}
		l := leaf{key: key, value: value}
		if t.root == nil {
			restart = t.lock.Upgrade(version, nil)
			if restart {
				continue
			}
			t.root = l
			t.lock.Unlock()
			return
		}
		if t.root.isLeaf() {
			restart = t.lock.Upgrade(version, nil)
			if restart {
				continue
			}
			t.root = t.root.insert(l, 0)
			t.lock.Unlock()
			return
		}
		restart = t.lock.RUnlock(version, nil)
		if restart {
			continue
		}
		t.root = t.root.insert(l, 0)
	}
}

func (t *Tree) Get(key []byte) (ValueType, bool) {
	var (
		version uint64
		restart = true
	)
	for restart {
		version, restart = t.lock.RLock()
		if restart {
			continue
		}
		root := t.root
		restart = t.lock.RUnlock(version, nil)
		if restart {
			continue
		}
		if root == nil {
			return nil, false
		}
		return root.get(key, 0)
	}
	panic("not reachable")
}

func (t *Tree) Delete(key []byte) {
	t.root = t.root.del(key, 0)
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
