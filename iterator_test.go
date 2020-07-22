package art

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {

	keys := [][]byte{
		{1, 2, 3, 4},
		{1, 3, 4, 6},
		{1, 3, 4, 5},
		{1, 2, 6, 7},
	}
	sorted := make([][]byte, len(keys))
	copy(sorted, keys)

	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	for _, tc := range []struct {
		desc       string
		keys       [][]byte
		start, end []byte
		rst        [][]byte
	}{
		{
			desc: "full",
			keys: keys,
			rst:  sorted,
		},
		{
			desc: "empty",
			rst:  [][]byte{},
		},
		{
			desc: "matching leaf",
			keys: keys[:1],
			rst:  keys[:1],
		},
		{
			desc:  "non matching leaf",
			keys:  keys[:1],
			rst:   [][]byte{},
			start: []byte{1, 3},
		},
		{
			desc: "limited by end",
			keys: keys,
			end:  []byte{1, 2, 255},
			rst:  sorted[:2],
		},
		{
			desc:  "limited by start",
			keys:  keys,
			start: []byte{1, 2, 4},
			rst:   sorted[1:],
		},
		{
			desc:  "start to end",
			keys:  keys,
			start: []byte{1, 2, 255},
			end:   []byte{1, 3, 4, 5},
			rst:   sorted[2:3],
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			var tree Tree
			for _, key := range tc.keys {
				tree.Insert(key, key)
			}
			iter := tree.Iterator(tc.start, tc.end)
			rst := [][]byte{}
			for iter.Next() {
				rst = append(rst, iter.Key())
			}
			require.Equal(t, tc.rst, rst)
		})
	}
}

func TestIteratorConcurrentExpansion(t *testing.T) {
	var (
		tree Tree
		keys = [][]byte{
			[]byte("aaba"),
			[]byte("aabb"),
		}
	)

	for _, key := range keys {
		tree.Insert(key, key)
	}
	iter := tree.Iterator(nil, nil)
	require.True(t, iter.Next())
	require.Equal(t, keys[0], iter.Key())

	tree.Insert([]byte("aaca"), nil)
	require.True(t, iter.Next())
	require.Equal(t, keys[1], iter.Key())
	require.True(t, iter.Next())
	require.Equal(t, []byte("aaca"), iter.Key())
}
