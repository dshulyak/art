package art

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComparePrefix(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		key1, key2 []byte
		off1, off2 int
		rst        int
	}{
		{
			desc: "eq no offset",
			key1: []byte{1, 2},
			key2: []byte{1, 2},
			rst:  2,
		},
		{
			desc: "key 1 shorter",
			key1: []byte{1, 2},
			key2: []byte{1, 2, 3},
			rst:  2,
		},
		{
			desc: "key 2 shorter",
			key1: []byte{1, 2, 3},
			key2: []byte{1, 2},
			rst:  2,
		},
		{
			desc: "unequal",
			key1: []byte{3, 2, 3},
			key2: []byte{1, 2},
		},
		{
			desc: "offset longer than key1",
			key1: []byte{1, 2},
			key2: []byte{1, 2},
			off1: 3,
		},
		{
			desc: "offset longer than key2",
			key1: []byte{1, 2},
			key2: []byte{1, 2},
			off2: 3,
		},
		{
			desc: "longer than max prefix",
			key1: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			key2: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			rst:  maxPrefixLen,
		},
		{
			desc: "shorter than max prefix by one",
			key1: []byte{1, 2, 3, 4, 5, 6, 7, 10},
			key2: []byte{1, 2, 3, 4, 5, 6, 7, 11},
			rst:  7,
		},
		{
			desc: "different offsets",
			key1: []byte{3, 1, 2},
			key2: []byte{1, 2, 1, 2},
			off1: 1,
			off2: 2,
			rst:  2,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.rst, comparePrefix(tc.key1, tc.key2, tc.off1, tc.off2))
		})
	}
}

func TestNodeChilds(t *testing.T) {
	for _, tc := range []struct {
		desc string
		node inode
	}{
		{"node4", &node4{}},
		{"node16", &node16{}},
		{"node48", &node48{}},
		{"node256", &node256{}},
	} {
		tc := tc
		n := tc.node
		t.Run(tc.desc, func(t *testing.T) {
			var k byte
			order := []node{}
			expand := func(n inode) {
				for !n.full() {
					added := &inner{}
					order = append(order, added)
					n.addChild(k, added)
					k++
				}
			}
			expand(n)
			testChilds := func(n inode) {
				for i, added := range order {
					index, child := n.child(byte(i))
					require.Equal(t, i, index)
					require.Equal(t, added, child)
				}
			}
			testChilds(n)
			if gn := n.grow(); gn != nil {
				n = gn
				testChilds(n)
				expand(n)
			}
			reduce := func(n inode) {
				for {
					min := n.min()
					k--
					i, _ := n.child(k)
					n.replace(i, nil)
					order = order[:len(order)-1]
					if min {
						break
					}
				}
			}
			reduce(n)
			n = n.shrink()
			if n != nil {
				testChilds(n)
			}
		})
	}
}

func TestMaxPrefixRecursive(t *testing.T) {
	a := [20]byte{}
	a[19] = 1
	b := [20]byte{}

	l1 := &leaf{key: a[:]}
	l2 := &leaf{key: b[:]}
	root, _ := l1.insert(l2, 0, nil, 0)

	// test that multiple levels were created
	root.walk(func(n node, depth int) bool {
		return true
	}, 0)
}
