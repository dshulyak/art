package art

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComparePrefix(t *testing.T) {
	p1 := []byte{1, 1, 1}
	p2 := []byte{2, 2, 2}
	require.Equal(t, 0, comparePrefix(p1, p2, 0))

	p1 = []byte{1, 1, 1, 1, 1, 1, 1, 1}
	p2 = []byte{1, 2}
	require.Equal(t, 1, comparePrefix(p1, p2, 0))
}

func TestInsertLeaf(t *testing.T) {
	val := 10
	var n node = leaf{key: []byte{7, 1, 2, 3}, value: val}
	n = n.insert(leaf{key: []byte{7, 1, 2, 4}}, 0)
	i, ok := n.(*inner)

	require.True(t, ok)
	require.Equal(t, []byte{7, 1, 2}, i.prefix[:i.prefixLen])

	rst := n.get([]byte{7, 1, 2, 3}, 0)
	require.Equal(t, val, rst)
}

func TestUncompress(t *testing.T) {
	var n node = &inner{
		prefix:    [maxPrefixLen]byte{7, 1, 2},
		prefixLen: 3,
		node:      &node4{},
	}
	n = n.insert(leaf{key: []byte{7, 3, 3, 7}}, 0)
	i, ok := n.(*inner)
	require.True(t, ok)
	require.Equal(t, i.prefix[:i.prefixLen], []byte{7})
	n.walk(func(n node, depth int) bool {
		padding := make([]byte, depth)
		for i := range padding {
			padding[i] = 0x2e
		}
		fmt.Printf("%s%s\n", string(padding), n)
		return true
	}, 0)
}

func TestNode4AddChild(t *testing.T) {
	n := node4{}
	n.addChild(4, nil)
	require.Equal(t, []byte{4, 0, 0, 0}, n.keys[:])
	n.addChild(3, nil)
	require.Equal(t, []byte{3, 4, 0, 0}, n.keys[:])
}
