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

	d, i, j := 0, off1, off2
	for i < k1lth && j < k2lth {
		if k1[i] != k2[j] {
			break
		}
		i++
		j++
		d++
		if d == maxPrefixLen {
			return d
		}
	}
	return d
}

// walkFn should return false if iteration should be terminated.
type walkFn func(node, int) bool

type node interface {
	insert(leaf, int, *olock, uint64) (node, bool)
	del([]byte, int, *olock, uint64, func(node)) bool
	get([]byte, int, *olock, uint64) (ValueType, bool, bool)
	walk(walkFn, int) bool
	inherit([maxPrefixLen]byte, int) node
	isLeaf() bool
	String() string
}

type inner struct {
	lock olock

	prefix    [maxPrefixLen]byte
	prefixLen int
	node      inode
}

func (n *inner) isLeaf() bool {
	return false
}

func (n *inner) walk(fn walkFn, depth int) bool {
	if !fn(n, depth) {
		return false
	}
	return n.node.walk(fn, depth+n.prefixLen+1)
}

func (n *inner) get(key []byte, depth int, parent *olock, parentVersion uint64) (value ValueType, found bool, obsolete bool) {
	var (
		version uint64
		restart = true
	)
	for restart {
		version, obsolete = n.lock.RLock()
		if obsolete || parent.RUnlock(parentVersion, nil) {
			return nil, false, true
		}
		cmp := comparePrefix(n.prefix[:n.prefixLen], key, 0, depth)
		if cmp != n.prefixLen {
			restart = n.lock.RUnlock(version, nil)
			if restart {
				continue
			}
			return nil, false, false
		}

		nextDepth := depth + n.prefixLen
		next := n.node.next(key[nextDepth])

		if next == nil {
			restart = n.lock.RUnlock(version, nil)
			if restart {
				continue
			}
			return nil, false, false
		}
		if next.isLeaf() {
			value, found, _ = next.get(key, nextDepth+1, &n.lock, version)
			restart = n.lock.RUnlock(version, nil)
			if restart {
				continue
			}
			return value, found, false
		}
		value, found, restart = next.get(key, nextDepth+1, &n.lock, version)
		if restart {
			continue
		}
		return value, found, false
	}
	panic("unreachable")
}

// insert
// TODO(dshulyak) no need to return pointer, the only case when we need to return pointer
// is when leaf has changed
func (n *inner) insert(l leaf, depth int, parent *olock, parentVersion uint64) (node, bool) {
	// NOTE(dshulyak) in this implementation we don't need to hold parent lock.
	// in the reference implementation parent needs to be updated with a different pointer if:
	// 1. node changed size
	// 2. prefix needs to be splitted
	// 3. path to leaf is uncompressed
	// 1 and 2 doesn't lead to change of pointer address in this implementation
	// 3 is handled by an explicit check that the next node is leaf and obtaining write lock before updating leaf
	var (
		version  uint64
		restart  = true
		obsolete bool
	)
	for restart {
		version, obsolete = n.lock.RLock()
		if obsolete || parent.RUnlock(parentVersion, nil) {
			return n, true
		}
		cmp := comparePrefix(n.prefix[:n.prefixLen], l.key, 0, depth)
		if cmp != n.prefixLen {
			restart = n.lock.Upgrade(version, nil)
			if restart {
				continue
			}
			child := &inner{
				prefixLen: n.prefixLen - cmp - 1,
				node:      n.node,
			}
			copy(child.prefix[:], n.prefix[cmp+1:])
			n.node = &node4{}
			n.node.addChild(l.key[depth+cmp], l)
			n.node.addChild(n.prefix[cmp], child)
			n.prefixLen = cmp
			n.lock.Unlock()
			return n, false
		}

		nextDepth := depth + n.prefixLen
		idx, next := n.node.child(l.key[nextDepth])

		if next == nil {
			restart = n.lock.Upgrade(version, nil)
			if restart {
				continue
			}
			if n.node.full() {
				n.node = n.node.grow()
			}
			n.node.addChild(l.key[nextDepth], l)
			n.lock.Unlock()
			return n, false
		}

		if next.isLeaf() {
			restart = n.lock.Upgrade(version, nil)
			if restart {
				continue
			}
			replacement, _ := next.insert(l, nextDepth+1, &n.lock, version)
			n.node.replace(idx, replacement)
			n.lock.Unlock()
			return n, false
		}

		_, restart = next.insert(l, nextDepth+1, &n.lock, version)
		if restart {
			continue
		}
		return n, false
	}
	panic("unreachable")
}

// del deletes the node with key and returns pointer for the parent to update himself.
// pointer may change if path is comressed:
// - either completely, pointer to the leaf will be returned
// - partially, e.g. prefixLen will be increased and prefixes merged
func (n *inner) del(key []byte, depth int, parent *olock, parentVersion uint64, replace func(node)) bool {
	var (
		version  uint64
		restart  = true
		obsolete bool
	)
	for restart {
		version, obsolete = n.lock.RLock()
		if obsolete || parent.RUnlock(parentVersion, nil) {
			return true
		}

		cmp := comparePrefix(n.prefix[:n.prefixLen], key, 0, depth)
		if cmp != n.prefixLen {
			// key is not found, check for concurrent writes and exit
			restart = n.lock.RUnlock(version, nil)
			if restart {
				continue
			}
			return false
		}

		nextDepth := depth + n.prefixLen
		idx, next := n.node.child(key[nextDepth])

		if next == nil {
			// key is not found, check for concurrent writes and exit
			restart = n.lock.RUnlock(version, nil)
			if restart {
				continue
			}
			return false
		}

		if l, isLeaf := next.(leaf); isLeaf && l.cmp(key) {
			_, isNode4 := n.node.(*node4)
			min := n.node.min()
			if isNode4 && min && n.prefixLen < maxPrefixLen {
				// update parent pointer. current node will be collapsed.
				restart = parent.Upgrade(parentVersion, nil)
				if restart {
					continue
				}
				restart = n.lock.Upgrade(version, parent)
				if restart {
					continue
				}

				n.node.replace(idx, nil)

				leftb, left := n.node.leftmost()
				n.prefix[n.prefixLen] = leftb
				n.prefixLen++
				replace(left.inherit(n.prefix, n.prefixLen))

				n.lock.UnlockObsolete()
				parent.Unlock()
				return false
			}
			// local change. parent lock won't be required
			restart = n.lock.Upgrade(version, nil)
			if restart {
				continue
			}
			n.node.replace(idx, nil)
			if min && !isNode4 {
				n.node = n.node.shrink()
			}
			n.lock.Unlock()
			return false
		} else if isLeaf {
			// key is not found. false-positive lookup due to compression.
			// check for concurrent writes and exit
			restart = n.lock.RUnlock(version, nil)
			if restart {
				continue
			}
			return false
		}

		restart = next.del(key, nextDepth+1, &n.lock, version, func(rn node) {
			n.node.replace(idx, rn)
		})
		if restart {
			continue
		}
		return false
	}
	panic("unreachable")
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

func (l leaf) isLeaf() bool {
	return true
}

func (l leaf) walk(fn walkFn, depth int) bool {
	return fn(l, depth)
}

func (l leaf) get(key []byte, depth int, parent *olock, parentVersion uint64) (ValueType, bool, bool) {
	if l.cmp(key) {
		return l.value, true, false
	}
	return nil, false, false
}

func (l leaf) cmp(other []byte) bool {
	return bytes.Compare(l.key, other) == 0
}

// insert updates leaf if key matches previous leaf or performs expansion if needed.
// expansion creates node4 and adds two leafs as childs.
func (l leaf) insert(other leaf, depth int, parent *olock, parentVersion uint64) (node, bool) {
	if other.cmp(l.key) {
		return other, false
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
	var zerolock olock
	_, _ = nn.insert(other, depth, &zerolock, 0)
	_, _ = nn.insert(l, depth, &zerolock, 0)
	return nn, false
}

func (l leaf) del([]byte, int, *olock, uint64, func(node)) bool {
	panic("not needed")
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
	// TODO refactor next/child and use one method
	// child should convert to int
	next(byte) node
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

func (n *node4) next(k byte) node {
	idx := n.index(k)
	if uint8(idx) == n.lth {
		return nil
	}
	if n.keys[idx] != k {
		return nil
	}
	return n.childs[idx]
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
	return n.lth <= 2
}

func (n *node4) shrink() inode {
	panic("can't shrink node4")
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
	// binary search is slower then loop 23ns > 16ns per op in worst case of scanning whole array
	// no reason to use binary search for non-vectorized version
	for i, b := range n.keys {
		if k <= b {
			return i
		}
	}
	return int(n.lth)
}

func (n *node16) child(k byte) (int, node) {
	idx, exist := index(&k, &n.keys)
	if !exist {
		return 0, nil
	}
	return idx, n.childs[idx]
}

func (n *node16) next(k byte) node {
	idx, exist := index(&k, &n.keys)
	if !exist {
		return nil
	}
	return n.childs[idx]
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
	return n.lth <= 5
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

func (n *node48) next(k byte) node {
	idx := n.keys[k]
	if idx == 0 {
		return nil
	}
	return n.childs[idx-1]
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
	return n.lth <= 17
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

func (n *node256) next(k byte) node {
	return n.childs[k]
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
	return n.lth <= 49
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
