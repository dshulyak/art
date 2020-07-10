package art

import (
	"bytes"
	"encoding/hex"
	"fmt"
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

	k1lth -= off1
	k2lth -= off2

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
	del([]byte, int) node
	get([]byte, int) (ValueType, bool)
	walk(walkFn, int) bool
	inherit([maxPrefixLen]byte, int) node
	String() string
}

type inner struct {
	prefix    [maxPrefixLen]byte
	prefixLen int
	node      inode
}

func (n *inner) walk(fn walkFn, depth int) bool {
	if !fn(n, depth) {
		return false
	}
	return n.node.walk(fn, depth+n.prefixLen+1)
}

func (n *inner) get(key []byte, depth int) (ValueType, bool) {
	cmp := comparePrefix(n.prefix[:n.prefixLen], key, 0, depth)
	if cmp != n.prefixLen {
		return nil, false
	}
	depth += n.prefixLen
	_, next := n.node.child(key[depth])
	if next == nil {
		return nil, false
	}
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
		copy(child.prefix[:], n.prefix[child.prefixLen:])
		n.node = &node4{}
		n.node.addChild(l.key[depth+cmp], l)
		n.node.addChild(n.prefix[cmp], child)
		n.prefixLen = cmp
		return n
	}
	depth += n.prefixLen
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

// del deletes the node with key and returns pointer for the parent to update himself.
// address of the pointer will be changed when nodes are merged and path is compressed
// or inner nodes are completely collapsed and pointer will refer to leaf
func (n *inner) del(key []byte, depth int) node {
	cmp := comparePrefix(n.prefix[:n.prefixLen], key, 0, depth)
	if cmp != n.prefixLen {
		return n
	}
	depth += n.prefixLen

	idx, next := n.node.child(key[depth])
	if next == nil {
		return n
	}
	n.node.replace(idx, next.del(key, depth+1))
	if n.node.min() {
		nn := n.node.shrink()
		// will be false only for node4
		if nn != nil {
			n.node = nn
			return n
		}
		// inner nodes with max prefix should be kept even if they have 1 child
		if n.prefixLen == maxPrefixLen {
			return n
		}
		// for node4 extend prefix or collapse inner nodes
		leftb, left := n.node.leftmost()
		n.prefix[n.prefixLen] = leftb
		n.prefixLen++
		return left.inherit(n.prefix, n.prefixLen)
	}
	return n
}

func (n *inner) inherit(prefix [maxPrefixLen]byte, prefixLen int) node {
	// two cases for inheritance of the prefix
	// 1. new prefixLen is <= max prefix len
	total := n.prefixLen + prefixLen
	if total <= maxPrefixLen {
		copy(prefix[prefixLen:], n.prefix[:])
		n.prefix = prefix
		n.prefixLen = total
		return n
	}
	// 2. >= max prefix len
	// resplit prefix, first part should have 8-byte length
	// second - leftover
	// pointer should use 9th byte
	// see long keys test
	nn := &inner{
		node: &node4{},
	}
	nn.prefix = prefix
	nn.prefixLen = maxPrefixLen
	copy(nn.prefix[prefixLen:], n.prefix[:])

	n.prefixLen = total - maxPrefixLen - 1
	kbyte := n.prefix[maxPrefixLen-prefixLen]
	copy(n.prefix[:], n.prefix[maxPrefixLen-prefixLen+1:])
	nn.node.addChild(kbyte, n)
	return nn
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

func (l leaf) get(key []byte, depth int) (ValueType, bool) {
	if l.cmp(key) {
		return l.value, true
	}
	return nil, false
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
	// see `long keys` test
	_ = nn.insert(other, depth)
	_ = nn.insert(l, depth)
	return nn
}

func (l leaf) del(key []byte, depth int) node {
	if l.cmp(key) {
		return nil
	}
	return l
}

func (l leaf) inherit([maxPrefixLen]byte, int) node {
	return l
}

func (l leaf) String() string {
	return fmt.Sprintf("leaf[%x]", l.key)
}

// inode is one of the inner nodes concrete representation
// node4/node16/node48/node256
type inode interface {
	child(byte) (int, node)
	// replace sets node at the index
	// if node is nil
	replace(int, node)
	full() bool
	grow() inode
	// min refers to the size of the node, should return true if size is less
	// then the minimum size
	min() bool
	// shrink is the opposite to grow
	// if node is of the smallest type (node4) nil will be returned
	shrink() inode
	// leftmost returns node with lowest index
	leftmost() (byte, node)
	addChild(byte, node)
	walk(walkFn, int) bool
	String() string
}

type node4 struct {
	lth    uint8
	keys   [4]byte
	childs [4]node
}

func (n *node4) index(k byte) int {
	for i, b := range n.keys {
		if k <= b {
			return i
		}
	}
	return int(n.lth)
}

func (n *node4) child(k byte) (int, node) {
	idx := n.index(k)
	if uint8(idx) == n.lth {
		return 0, nil
	}
	if n.keys[idx] != k {
		return idx, nil
	}
	return idx, n.childs[idx]
}

func (n *node4) replace(idx int, child node) {
	if child == nil {
		copy(n.keys[idx:], n.keys[idx+1:])
		copy(n.childs[idx:], n.childs[idx+1:])
		n.keys[n.lth-1] = 0
		n.childs[n.lth-1] = nil
		n.lth--
	} else {
		n.childs[idx] = child
	}
}

func (n *node4) leftmost() (byte, node) {
	return n.keys[0], n.childs[0]
}

func (n *node4) addChild(k byte, child node) {
	idx := n.index(k)
	copy(n.childs[idx+1:], n.childs[idx:])
	copy(n.keys[idx+1:], n.keys[idx:])
	n.keys[idx] = k
	n.childs[idx] = child
	n.lth++
}

func (n *node4) min() bool {
	return n.lth <= 1
}

func (n *node4) shrink() inode {
	return nil
}

func (n *node4) full() bool {
	return n.lth == 4
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
	// binary search is slow then loop 23ns > 16ns per op in worst case of scanning whole array
	// no reason to use binary search for non-vectorized version
	for i, b := range n.keys {
		if k <= b {
			return i
		}
	}
	return int(n.lth)
}

func (n *node16) child(k byte) (int, node) {
	idx := n.index(k)
	if uint8(idx) == n.lth {
		return 0, nil
	}
	if n.keys[idx] != k {
		return idx, nil
	}
	return idx, n.childs[idx]
}

func (n *node16) replace(idx int, child node) {
	if child == nil {
		copy(n.keys[idx:], n.keys[idx+1:])
		copy(n.childs[idx:], n.childs[idx+1:])
		n.keys[n.lth-1] = 0
		n.childs[n.lth-1] = nil
		n.lth--
	} else {
		n.childs[idx] = child
	}
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
	nn := &node48{
		lth: n.lth,
	}
	copy(nn.childs[:], n.childs[:])
	for i := range n.childs {
		nn.keys[n.keys[i]] = uint16(i) + 1
	}
	return nn
}

func (n *node16) min() bool {
	return n.lth <= 4
}

func (n *node16) shrink() inode {
	nn := node4{}
	copy(nn.keys[:], n.keys[:])
	copy(nn.childs[:], n.childs[:])
	nn.lth = n.lth
	return &nn
}

func (n *node16) leftmost() (byte, node) {
	return n.keys[0], n.childs[0]
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
	lth    uint8
	keys   [256]uint16
	childs [48]node
}

func (n *node48) child(k byte) (int, node) {
	idx := n.keys[k]
	if idx == 0 {
		return 0, nil
	}
	return int(idx) - 1, n.childs[idx-1]
}

func (n *node48) full() bool {
	return n.lth == 48
}

func (n *node48) addChild(k byte, child node) {
	n.keys[k] = uint16(n.lth + 1)
	n.childs[n.lth] = child
	n.lth++
}

func (n *node48) grow() inode {
	nn := &node256{
		lth: uint16(n.lth),
	}
	for b, i := range n.keys {
		if i == 0 {
			continue
		}
		nn.childs[b] = n.childs[i-1]
	}
	return nn
}

func (n *node48) replace(idx int, child node) {
	n.childs[idx] = child
	if child == nil {
		n.lth--
	}
}

func (n *node48) min() bool {
	return n.lth <= 16
}

func (n *node48) shrink() inode {
	nn := &node16{
		lth: n.lth,
	}
	nni := 0
	for i, index := range n.keys {
		if index == 0 {
			continue
		}
		child := n.childs[index-1]
		if child != nil {
			nn.keys[nni] = byte(i)
			nn.childs[nni] = child
			nni++
		}
	}
	return nn
}

func (n *node48) leftmost() (byte, node) {
	panic("not implemented")
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

func (n *node48) String() string {
	var b bytes.Buffer
	_, _ = b.WriteString("n48[")
	encoder := hex.NewEncoder(&b)
	for i, index := range n.keys {
		if index == 0 {
			continue
		}
		child := n.childs[index-1]
		if child != nil {
			_, _ = encoder.Write([]byte{byte(i)})
		}
	}
	_, _ = b.WriteString("]")
	return b.String()
}

type node256 struct {
	lth    uint16
	childs [256]node
}

func (n *node256) child(k byte) (int, node) {
	return int(k), n.childs[k]
}

func (n *node256) replace(idx int, child node) {
	n.childs[byte(idx)] = child
	if child == nil {
		n.lth--
	}
}

func (n *node256) full() bool {
	return n.lth == 256
}

func (n *node256) addChild(k byte, child node) {
	n.childs[k] = child
	n.lth++
}

func (n *node256) grow() inode {
	return nil
}

func (n *node256) min() bool {
	return n.lth <= 48
}

func (n *node256) shrink() inode {
	nn := &node48{
		lth: uint8(n.lth),
	}
	var index uint16
	for i := range n.childs {
		if n.childs[i] == nil {
			continue
		}
		index++
		nn.keys[i] = index
		nn.childs[index-1] = n.childs[i]
	}
	return nn
}

func (n *node256) leftmost() (byte, node) {
	panic("not implemented")
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

func (n *node256) String() string {
	var b bytes.Buffer
	_, _ = b.WriteString("n256[")
	encoder := hex.NewEncoder(&b)
	for i := range n.childs {
		if n.childs[i] != nil {
			_, _ = encoder.Write([]byte{byte(i)})
		}
	}
	_, _ = b.WriteString("]")
	return b.String()
}
