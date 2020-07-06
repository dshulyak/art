package art

type ValueType interface{}

type Tree struct {
	root node
}

func (t *Tree) Insert(key []byte, value ValueType) {
	l := leaf{key: key, value: value}
	if t.root == nil {
		t.root = l
	}
	t.root = t.root.insert(l, 0)
}

func (t *Tree) Get(key []byte) ValueType {
	if t.root == nil {
		return nil
	}
	return t.root.get(key, 0)
}
