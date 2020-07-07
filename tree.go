package art

import (
	"bytes"
)

type ValueType interface{}

type Tree struct {
	root node
}

func (t *Tree) Insert(key []byte, value ValueType) {
	l := leaf{key: key, value: value}
	if t.root == nil {
		t.root = l
		return
	}
	t.root = t.root.insert(l, 0)
}

func (t *Tree) Get(key []byte) ValueType {
	if t.root == nil {
		return nil
	}
	return t.root.get(key, 0)
}

func (t *Tree) Pretty() string {
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
