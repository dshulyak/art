package art

import (
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
	n := &node{
		node: leaf{key: []byte{7, 1, 2, 3}},
	}
	n.insert(leaf{key: []byte{7, 1, 2, 4}}, 0)
	require.Equal(t, []byte{7, 1, 2}, n.prefix[:n.prefixLen])
}

func TestUncompress(t *testing.T) {
	n := &node{
		prefix:    [maxPrefixLen]byte{7, 1, 2},
		prefixLen: 3,
		node:      &node4{},
	}
	n.insert(leaf{key: []byte{7, 3, 3, 7}}, 0)
	require.Equal(t, n.prefix[:n.prefixLen], []byte{7})
}
