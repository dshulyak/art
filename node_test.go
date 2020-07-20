package art

import (
	"fmt"
	"math/rand"
	"sync"
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

func TestNodesConcurrentShrink(t *testing.T) {
	var (
		root     = &inner{node: &node4{}}
		rootLock olock
	)
	for i := 0; i < 48; i++ {
		_, restart := root.insert(&leaf{key: []byte{byte(i)}}, 0, &rootLock, 0)
		require.False(t, restart)
	}
	version1, _ := rootLock.RLock()
	rootLock.Lock()
	rootLock.Unlock()
	restart := root.del([]byte{0}, 0, &rootLock, version1, func(rn node) {
		root.node.replace(0, rn)
	})
	require.True(t, restart)
	version2, _ := rootLock.RLock()
	_, restart = root.insert(&leaf{key: []byte{50}}, 0, &rootLock, version2)
	require.False(t, restart)
	version3, _ := rootLock.RLock()
	restart = root.del([]byte{0}, 0, &rootLock, version3, func(rn node) {
		root.node.replace(0, rn)
	})
	require.False(t, restart)
}

func TestNode48Insert(t *testing.T) {
	var (
		n48    = &node48{}
		n      = inner{node: n48}
		parent olock
	)
	keys := make([][]byte, 48)
	for i := range keys {
		key := make([]byte, 20)
		rand.Read(key)
		keys[i] = key
	}
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(key []byte) {
			for i := 0; i < 100; i++ {
				n.insert(&leaf{key: key}, 0, &parent, 0)
			}
			wg.Done()
		}(key)
	}
	wg.Wait()
	fmt.Println(n48.childs)
}

func TestNode48InsertDelete(t *testing.T) {
	var (
		n48    = &node48{}
		n      = inner{node: n48}
		parent olock
	)
	keys := make([][]byte, 16)
	for i := range keys {
		key := make([]byte, 20)
		rand.Read(key)
		keys[i] = key
	}
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(key []byte) {
			for i := 0; i < 100; i++ {
				n.insert(&leaf{key: key}, 0, &parent, 0)
			}
			wg.Done()
		}(key)
	}

	for _, key := range keys[15:] {
		wg.Add(1)
		go func(key []byte) {
			for i := 0; i < 100; i++ {
				n.del(key, 0, &parent, 0, func(node) {
				})
			}
			wg.Done()
		}(key)
	}

	wg.Wait()
}
