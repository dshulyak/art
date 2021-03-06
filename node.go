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
	insert(*leaf, int, *olock, uint64) (node, bool)
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

func (n *inner) get(key []byte, depth int, parent *olock, parentVersion uint64) (ValueType, bool, bool) {
	for {
		version, obsolete := n.lock.RLock()
		if obsolete || parent.RUnlock(parentVersion, nil) {
			return nil, false, true
		}
		cmp := comparePrefix(n.prefix[:n.prefixLen], key, 0, depth)
		if cmp != n.prefixLen {
			if n.lock.RUnlock(version, nil) {
				continue
			}
			return nil, false, false
		}

		nextDepth := depth + n.prefixLen
		_, next := n.node.child(key[nextDepth])

		if next == nil {
			if n.lock.RUnlock(version, nil) {
				continue
			}
			return nil, false, false
		}
		if next.isLeaf() {
			value, found, _ := next.get(key, nextDepth+1, &n.lock, version)
			if n.lock.RUnlock(version, nil) {
				continue
			}
			return value, found, false
		}
		value, found, restart := next.get(key, nextDepth+1, &n.lock, version)
		if restart {
			continue
		}
		return value, found, false
	}
	panic("unreachable")
}

// insert ...
func (n *inner) insert(l *leaf, depth int, parent *olock, parentVersion uint64) (node, bool) {
	for {
		version, obsolete := n.lock.RLock()
		if obsolete {
			return n, true
		}
		cmp := comparePrefix(n.prefix[:n.prefixLen], l.key, 0, depth)
		if cmp != n.prefixLen {
			// parent lock is required
			// because parent may collapse and child will have
			// to `inherit` parent prefix
			// `inherit` routine updates prefixLen and prefix
			if parent.Upgrade(parentVersion, nil) {
				return nil, true
			}
			if n.lock.Upgrade(version, parent) {
				return nil, true
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
			parent.Unlock()
			return n, false
		}

		nextDepth := depth + n.prefixLen
		idx, next := n.node.child(l.key[nextDepth])

		if next == nil {
			if n.lock.Upgrade(version, nil) {
				continue
			}
			if parent.RUnlock(parentVersion, &n.lock) {
				return n, true
			}
			if n.node.full() {
				n.node = n.node.grow()
			}
			n.node.addChild(l.key[nextDepth], l)
			n.lock.Unlock()
			return n, false
		}
		if parent.RUnlock(parentVersion, nil) {
			return n, true
		}
		if next.isLeaf() {
			if n.lock.Upgrade(version, nil) {
				continue
			}

			replacement, _ := next.insert(l, nextDepth+1, &n.lock, version)
			n.node.replace(idx, replacement)
			n.lock.Unlock()
			return n, false
		}

		_, restart := next.insert(l, nextDepth+1, &n.lock, version)
		if restart {
			continue
		}
		return n, false
	}
	panic("unreachable")
}

// del deletes the node with key and returns pointer for the parent for update.
// pointer may change if path is comressed:
// - either completely, pointer to the leaf will be returned
// - partially, e.g. prefixLen will be increased and prefixes merged
func (n *inner) del(key []byte, depth int, parent *olock, parentVersion uint64, replace func(node)) bool {
	for {
		version, obsolete := n.lock.RLock()
		if obsolete {
			return true
		}

		cmp := comparePrefix(n.prefix[:n.prefixLen], key, 0, depth)
		if cmp != n.prefixLen {
			// key is not found, check for concurrent writes and exit
			if n.lock.RUnlock(version, nil) {
				continue
			}
			return parent.RUnlock(parentVersion, nil)
		}

		nextDepth := depth + n.prefixLen
		idx, next := n.node.child(key[nextDepth])
		if next == nil {
			// key is not found, check for concurrent writes and exit
			if n.lock.RUnlock(version, nil) {
				continue
			}
			return parent.RUnlock(parentVersion, nil)
		}

		if l, isLeaf := next.(*leaf); isLeaf && l.cmp(key) {
			_, isNode4 := n.node.(*node4)
			min := n.node.min()
			if isNode4 && min && n.prefixLen < maxPrefixLen {
				// update parent pointer. current node will be collapsed.
				if parent.Upgrade(parentVersion, nil) {
					return true
				}
				if n.lock.Upgrade(version, parent) {
					// need to update parent version
					return true
				}

				n.node.replace(idx, nil)

				leftb, left := n.node.next(nil)
				n.prefix[n.prefixLen] = leftb
				n.prefixLen++

				replace(left.inherit(n.prefix, n.prefixLen))

				n.lock.Unlock()
				parent.Unlock()
				return false
			}
			// local change. parent lock won't be required
			if n.lock.Upgrade(version, nil) {
				continue
			}
			if parent.RUnlock(parentVersion, &n.lock) {
				return true
			}
			n.node.replace(idx, nil)
			if min && !isNode4 {
				n.node = n.node.shrink()
			}
			n.lock.Unlock()
			return false
		} else if isLeaf {
			// key is not found. check for concurrent writes and exit
			if n.lock.RUnlock(version, nil) {
				continue
			}
			return parent.RUnlock(parentVersion, nil)
		}

		if parent.RUnlock(parentVersion, nil) {
			return true
		}

		if next.del(key, nextDepth+1, &n.lock, version, func(rn node) {
			n.node.replace(idx, rn)
		}) {
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

func (l *leaf) isLeaf() bool {
	return true
}

func (l *leaf) walk(fn walkFn, depth int) bool {
	return fn(l, depth)
}

func (l *leaf) get(key []byte, depth int, parent *olock, parentVersion uint64) (ValueType, bool, bool) {
	if l.cmp(key) {
		return l.value, true, false
	}
	return nil, false, false
}

func (l *leaf) cmp(other []byte) bool {
	return bytes.Compare(l.key, other) == 0
}

// insert updates leaf if key matches previous leaf or performs expansion if needed.
// expansion creates node4 and adds two leafs as childs
func (l *leaf) insert(other *leaf, depth int, parent *olock, parentVersion uint64) (node, bool) {
	if other.cmp(l.key) {
		return other, false
	}

	var (
		head *inner
		prev *inner
	)
	for {
		cmp := comparePrefix(l.key, other.key, depth, depth)
		nn := &inner{
			prefixLen: cmp,
			node:      &node4{},
		}

		copy(nn.prefix[:], l.key[depth:depth+cmp])

		if head == nil {
			head = nn
		}

		if prev != nil {
			prev.node.addChild(l.key[depth-1], nn)
		}

		if cmp < maxPrefixLen {
			nn.node.addChild(l.key[depth+cmp], l)
			nn.node.addChild(other.key[depth+cmp], other)
			break
		}
		prev = nn
		depth += cmp + 1
	}
	return head, false
}

func (l *leaf) del([]byte, int, *olock, uint64, func(node)) bool {
	panic("not needed")
}

func (l *leaf) inherit([maxPrefixLen]byte, int) node {
	return l
}

func (l *leaf) String() string {
	return fmt.Sprintf("leaf[%x]", l.key)
}

// inode is one of the inner nodes concrete representation
// node4/node16/node48/node256
type inode interface {
	// next returns child after the requested byte
	// if byte is nil - returns leftmost child
	next(*byte) (byte, node)
	prev(*byte) (byte, node)

	// child return index of the child together with the child
	child(byte) (int, node)
	// addChild inserts child at the specified byte
	addChild(byte, node)
	// replace updates node at specified index
	// if node is nil - delete the node and adjust metadata
	replace(int, node)

	// full is true if node reached max size
	full() bool
	// grow the node to next size
	// node256 can't grow and will return nil
	grow() inode

	// min is true if node reached min size
	min() bool
	// shrink is the opposite to grow
	// if node is of the smallest type (node4) nil will be returned
	shrink() inode

	// walk is internal helper to iterate in depth first order over all nodes, including inner nodes
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

func (n *node4) next(k *byte) (byte, node) {
	if k == nil {
		return n.keys[0], n.childs[0]
	}
	for idx, b := range n.keys {
		if b > *k {
			return b, n.childs[idx]
		}
	}
	return 0, nil
}

func (n *node4) prev(k *byte) (byte, node) {
	if n.lth == 0 {
		return 0, nil
	}
	if k == nil {
		idx := n.lth - 1
		return n.keys[idx], n.childs[idx]
	}
	for i := n.lth; i > 0; i-- {
		idx := i - 1
		if n.keys[idx] < *k {
			return n.keys[idx], n.childs[idx]
		}
	}
	return 0, nil
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

func (n *node16) next(k *byte) (byte, node) {
	if k == nil {
		return n.keys[0], n.childs[0]
	}
	for i, b := range n.keys {
		if b > *k {
			return b, n.childs[i]
		}
	}
	return 0, nil
}

func (n *node16) prev(k *byte) (byte, node) {
	if k == nil {
		idx := n.lth - 1
		return n.keys[idx], n.childs[idx]
	}
	for i := n.lth; i >= 0; i-- {
		idx := i - 1
		if n.keys[idx] < *k {
			return n.keys[idx], n.childs[idx]
		}
	}
	return 0, nil
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
	for i, child := range n.childs {
		if child == nil {
			continue
		}
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
	return int(k), n.childs[idx-1]
}

func (n *node48) next(k *byte) (byte, node) {
	for b, idx := range n.keys {
		if (k == nil || byte(b) > *k) && idx != 0 {
			return byte(b), n.childs[idx-1]
		}
	}
	return 0, nil
}

func (n *node48) prev(k *byte) (byte, node) {
	for b := n.lth - 1; b >= 0; b-- {
		idx := n.keys[b]
		if (k == nil || byte(b) < *k) && idx != 0 {
			return byte(b), n.childs[idx]
		}
	}
	return 0, nil
}

func (n *node48) full() bool {
	return n.lth == 48
}

func (n *node48) addChild(k byte, child node) {
	for idx, existing := range n.childs {
		if existing == nil {
			n.keys[k] = uint16(idx + 1)
			n.childs[idx] = child
			n.lth++
			return
		}
	}
	panic("no empty slots")
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

func (n *node48) replace(k int, child node) {
	idx := n.keys[k]
	if idx == 0 {
		panic("replace can't be called for idx=0")
	}
	n.childs[idx-1] = child
	if child == nil {
		n.keys[k] = 0
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

func (n *node256) next(k *byte) (byte, node) {
	for b, child := range n.childs {
		if (k == nil || byte(b) > *k) && child != nil {
			return byte(b), child
		}
	}
	return 0, nil
}

func (n *node256) prev(k *byte) (byte, node) {
	for idx := n.lth - 1; idx >= 0; idx-- {
		b := byte(idx)
		child := n.childs[idx]
		if (k == nil || b < *k) && child != nil {
			return b, child
		}
	}
	return 0, nil
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
