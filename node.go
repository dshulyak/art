package art

import (
	"bytes"
	"sort"
)

const (
	maxPrefixLen int = 8
)

func comparePrefix(k1, k2 []byte, offset int) int {
	i := offset
	for ; i < len(k1) && i < len(k2); i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	if i-offset > maxPrefixLen {
		return maxPrefixLen - 1
	}
	return i - offset
}

type node interface {
	insert(leaf, int) node
}

type inner struct {
	prefix    [maxPrefixLen]byte
	prefixLen int
	node      inode
}

func (n *inner) insert(l leaf, depth int) node {
	// FIXME out of bounds in l.key[depth:]
	// uncompress path if needed
	cmp := comparePrefix(n.prefix[:n.prefixLen], l.key[depth:], 0)
	if cmp != n.prefixLen {
		child := &inner{
			prefixLen: n.prefixLen - cmp,
			node:      n.node,
		}
		copy(child.prefix[:], n.prefix[cmp:])
		n.node = &node4{}
		n.node.addChild(l.key[depth+cmp], l)
		n.node.addChild(n.prefix[cmp], child)
		n.prefixLen = cmp
		return n
	}
	// normal insertion flow
	depth += n.prefixLen
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

type leaf struct {
	key   []byte
	value interface{}
}

func (l leaf) cmp(other leaf) bool {
	return bytes.Compare(l.key, other.key) == 0
}

func (l leaf) insert(other leaf, depth int) node {
	if other.cmp(l) {
		return other
	}
	cmp := comparePrefix(l.key, other.key, depth)
	nn := &inner{
		prefixLen: cmp,
		node:      &node4{},
	}
	copy(nn.prefix[:], l.key[depth:depth+cmp])
	depth += nn.prefixLen
	nn.node.addChild(l.key[depth], l)
	nn.node.addChild(other.key[depth], other)
	return nn
}

// inode is one of the inner nodes representation
type inode interface {
	child(byte) (int, node)
	replace(int, node)
	full() bool
	grow() inode
	addChild(byte, node)
}

type node4 struct {
	keys   [4]byte
	childs [4]node
}

func (n *node4) index(k byte) int {
	for i, b := range n.keys {
		if k <= b {
			return i
		}
	}
	return 0
}

func (n *node4) child(k byte) (int, node) {
	idx := n.index(k)
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
}

func (n *node4) full() bool {
	return n.childs[3] != nil
}

func (n *node4) grow() inode {
	nn := &node16{}
	copy(nn.keys[:], n.keys[:])
	copy(nn.childs[:], n.childs[:])
	return nn
}

type node16 struct {
	lth    int
	keys   [16]byte
	childs [16]node
}

func (n *node16) index(k byte) int {
	return sort.Search(n.lth, func(i int) bool {
		return n.keys[i] >= k
	})
}

func (n *node16) child(k byte) (int, node) {
	idx := n.index(k)
	return idx, n.childs[idx]
}

func (n *node16) replace(idx int, child node) {
	n.childs[idx] = child
}

func (n *node16) full() bool {
	return n.lth == 16
}

func (n *node16) addChild(k byte, child node) {
	n.lth++
	idx := n.index(k)
	copy(n.childs[idx+1:], n.childs[idx:])
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = k
	n.childs[idx] = child
}

func (n *node16) grow() inode {
	nn := &node48{}
	copy(nn.childs[:], n.childs[:])
	for i := range n.childs {
		nn.keys[n.keys[i]] = i
	}
	return nn
}

type node48 struct {
	next   int
	keys   [256]int
	childs [48]node
}

func (n *node48) child(k byte) (int, node) {
	idx := n.keys[k]
	return idx, n.childs[idx]
}

func (n *node48) full() bool {
	return n.next == 48
}

func (n *node48) addChild(k byte, child node) {
	n.keys[k] = n.next
	n.childs[n.next] = child
	n.next++
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
