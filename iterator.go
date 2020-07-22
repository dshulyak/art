package art

import (
	"bytes"
)

type checkpoint struct {
	node *inner

	pointer *byte
}

// iterator will scan the tree in lexicographic order.
// not concurrenctly safe
type iterator struct {
	tree *Tree

	stack []*checkpoint

	start, end []byte

	closed bool

	key   []byte
	value ValueType
}

// Next will iterate over all leaf nodes inbetween specified prefixes
func (i *iterator) Next() bool {
	if i.closed {
		return false
	}
	if i.stack == nil {
		// iterator wasn't initialized yet
		root := i.tree.root
		if root == nil {
			i.closed = true
			return false
		}
		l, isLeaf := root.(*leaf)
		if isLeaf {
			i.closed = true
			cmp := bytes.Compare(l.key, i.start)
			if cmp >= 0 && (i.end == nil || bytes.Compare(l.key, i.end) <= 0) {
				i.key = l.key
				i.value = l.value
				return true
			}
			return false
		}
		i.stack = []*checkpoint{{node: root.(*inner)}}
	}

	for len(i.stack) > 0 {
		lth := len(i.stack)
		tail := i.stack[lth-1]

		pointer, child := tail.node.node.next(tail.pointer)
		if child == nil {
			// current checkpoint was exhausted, continue until leaf is found
			// or stack is empty
			i.stack[lth-1] = nil
			i.stack = i.stack[:lth-1]
			continue
		}
		tail.pointer = &pointer
		l, isLeaf := child.(*leaf)
		if isLeaf {
			cmp := bytes.Compare(l.key, i.start)
			if cmp >= 0 && (i.end == nil || bytes.Compare(l.key, i.end) <= 0) {
				i.key = l.key
				i.value = l.value
				return true
			}
			continue
		}
		i.stack = append(i.stack, &checkpoint{
			node: child.(*inner),
		})
	}
	i.closed = true
	return false
}

func (i *iterator) Value() ValueType {
	return i.value
}

func (i *iterator) Key() []byte {
	return i.key
}
