package art

import (
	"bytes"
	"fmt"
	"sort"
)

const (
	maxPrefixLen int = 8
)

func comparePrefix(k1, k2 []byte, off1, off2 int) int {
	k1lth := len(k1)
	k2lth := len(k2)

	if off1 < k1lth {
		k1 = k1[off1:]
	} else {
		return 0
	}
	if off2 < k2lth {
		k2 = k2[off2:]
	} else {
		return 0
	}

	i := 0
	for ; i < k1lth && i < k2lth; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	if i > maxPrefixLen {
		return maxPrefixLen
	}
	return i
}

// walkFn should return false if iteration should be terminated.
type walkFn func(node, int) bool

type node interface {
	insert(leaf, int) node
	get([]byte, int) ValueType
	walk(walkFn, int) bool
	String() string
}

type inner struct {
	prefix    [maxPrefixLen]byte
	prefixLen int
	node      inode

	// null is an additional pointer for storing leaf with a key that
	// terminates on the depth of this node
	null node
}

func (n *inner) walk(fn walkFn, depth int) bool {
	if !fn(n, depth) {
		return false
	}
	if n.null != nil && !fn(n.null, depth+n.prefixLen+1) {
		return false
	}
	return n.node.walk(fn, depth+n.prefixLen+1)
}

func (n *inner) get(key []byte, depth int) ValueType {
	cmp := comparePrefix(n.prefix[:n.prefixLen], key, 0, depth)
	if cmp != n.prefixLen {
		return nil
	}
	depth += n.prefixLen
	if depth == len(key) {
		return n.null.get(key, depth+1)
	}
	_, next := n.node.child(key[depth])
	return next.get(key, depth+1)
}

func (n *inner) insert(l leaf, depth int) node {
	// uncompress path
	cmp := comparePrefix(n.prefix[:n.prefixLen], l.key, 0, depth)
	if cmp != n.prefixLen {
		child := &inner{
			prefixLen: n.prefixLen - cmp - 1,
			node:      n.node,
		}
		copy(child.prefix[:], n.prefix[cmp:])
		n.node = &node4{}
		n.node.addChild(l.key[depth+cmp], l)
		n.node.addChild(n.prefix[cmp], child)
		n.prefixLen = cmp
		return n
	}
	depth += n.prefixLen
	if len(l.key) == depth {
		n.null = l
		return n
	}
	// normal insertion flow
	idx, next := n.node.child(l.key[depth])
	if next != nil {
		n.node.replace(idx, next.insert(l, depth+1))
	} else {
		if n.node.full() {
			n.node = n.node.grow()
		}
		n.node.addChild(l.key[depth], l)
	}
	return n
}

func (n *inner) String() string {
	return fmt.Sprintf("inner[%x]%s", n.prefix[:n.prefixLen], n.node)
}

type leaf struct {
	key   []byte
	value ValueType
}

func (l leaf) walk(fn walkFn, depth int) bool {
	return fn(l, depth)
}

func (l leaf) get(key []byte, depth int) ValueType {
	if l.cmp(key) {
		return l.value
	}
	return nil
}

func (l leaf) cmp(other []byte) bool {
	return bytes.Compare(l.key, other) == 0
}

// insert updates leaf if key matches previous leaf or performs expansion if needed.
// expansion creates node4 and adds two leafs as childs.
func (l leaf) insert(other leaf, depth int) node {
	if other.cmp(l.key) {
		return other
	}
	cmp := comparePrefix(l.key, other.key, depth, depth)
	nn := &inner{
		prefixLen: cmp,
		node:      &node4{},
	}
	key := l.key
	if len(other.key) > len(key) {
		key = other.key
	}
	copy(nn.prefix[:], key[depth:depth+cmp])
	// max prefix length is 8 byte, if common prefix longer than
	// that then multiple inner nodes will be inserted
	// see `log keys` test
	_ = nn.insert(other, depth)
	_ = nn.insert(l, depth)
	return nn
}

func (l leaf) String() string {
	return fmt.Sprintf("leaf[%x]", l.key)
}

// inode is one of the inner nodes concrete representation
// node4/node16/node48/node256
type inode interface {
	child(byte) (int, node)
	replace(int, node)
	full() bool
	grow() inode
	addChild(byte, node)
	walk(walkFn, int) bool
}

type node4 struct {
	lth    uint8
	keys   [4]byte
	childs [4]node
}

func (n *node4) index(k byte) int {
	return sort.Search(int(n.lth), func(i int) bool {
		return n.keys[i] >= k
	})
}

func (n *node4) child(k byte) (int, node) {
	idx := n.index(k)
	if n.keys[idx] != k {
		return idx, nil
	}
	return idx, n.childs[idx]
}

func (n *node4) replace(idx int, child node) {
	n.childs[idx] = child
}

func (n *node4) addChild(k byte, child node) {
	idx := n.index(k)
	copy(n.childs[idx+1:], n.childs[idx:])
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = k
	n.childs[idx] = child
	n.lth++
}

func (n *node4) full() bool {
	return n.childs[3] != nil
}

func (n *node4) grow() inode {
	nn := &node16{}
	nn.lth = n.lth
	copy(nn.keys[:], n.keys[:])
	copy(nn.childs[:], n.childs[:])
	return nn
}

func (n *node4) walk(fn walkFn, depth int) bool {
	for i := range n.childs {
		if uint8(i) < n.lth {
			if !n.childs[i].walk(fn, depth) {
				return false
			}
		}
	}
	return true
}

func (n *node4) String() string {
	return fmt.Sprintf("n4[%x]", n.keys[:n.lth])
}

type node16 struct {
	lth    uint8
	keys   [16]byte
	childs [16]node
}

func (n *node16) index(k byte) int {
	return sort.Search(int(n.lth), func(i int) bool {
		return n.keys[i] >= k
	})
}

func (n *node16) child(k byte) (int, node) {
	idx := n.index(k)
	if n.keys[idx] != k {
		return idx, nil
	}
	return idx, n.childs[idx]
}

func (n *node16) replace(idx int, child node) {
	n.childs[idx] = child
}

func (n *node16) full() bool {
	return n.lth == 16
}

func (n *node16) addChild(k byte, child node) {
	idx := n.index(k)
	copy(n.childs[idx+1:], n.childs[idx:])
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = k
	n.childs[idx] = child
	n.lth++
}

func (n *node16) grow() inode {
	nn := &node48{}
	copy(nn.childs[:], n.childs[:])
	for i := range n.childs {
		nn.keys[n.keys[i]] = i
	}
	return nn
}

func (n *node16) walk(fn walkFn, depth int) bool {
	for i := range n.childs {
		if uint8(i) < n.lth {
			if !n.childs[i].walk(fn, depth) {
				return false
			}
		}
	}
	return true
}

func (n *node16) String() string {
	return fmt.Sprintf("n16[%x]", n.keys[:n.lth])
}

type node48 struct {
	lth    int
	keys   [256]int
	childs [48]node
}

func (n *node48) child(k byte) (int, node) {
	idx := n.keys[k]
	return idx, n.childs[idx]
}

func (n *node48) full() bool {
	return n.lth == 48
}

func (n *node48) addChild(k byte, child node) {
	n.keys[k] = n.lth
	n.childs[n.lth] = child
	n.lth++
}

func (n *node48) grow() inode {
	nn := &node256{}
	for b, i := range n.keys {
		nn.childs[b] = n.childs[i]
	}
	return nn
}

func (n *node48) replace(idx int, child node) {
	n.childs[idx] = child
}

func (n *node48) walk(fn walkFn, depth int) bool {
	for _, child := range n.childs {
		if child != nil {
			if !child.walk(fn, depth) {
				return false
			}
		}
	}
	return true
}

type node256 struct {
	childs [256]node
}

func (n *node256) child(k byte) (int, node) {
	return int(k), n.childs[k]
}

func (n *node256) replace(idx int, child node) {
	n.childs[byte(idx)] = child
}

func (n *node256) full() bool {
	return false
}

func (n *node256) addChild(k byte, child node) {
	n.childs[k] = child
}

func (n *node256) grow() inode {
	panic("node256 won't grow")
}

func (n *node256) walk(fn walkFn, depth int) bool {
	for _, child := range n.childs {
		if child != nil {
			if !child.walk(fn, depth) {
				return false
			}
		}
	}
	return true
}
