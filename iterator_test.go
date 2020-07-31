package art

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {

	keys := []string{
		"1234",
		"1245",
		"1345",
		"1267",
	}
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	reversed := make([]string, len(keys))
	copy(reversed, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))

	for _, tc := range []struct {
		desc       string
		keys       []string
		start, end string
		reverse    bool
		rst        []string
	}{
		{
			desc: "full",
			keys: keys,
			rst:  sorted,
		},
		{
			desc: "empty",
			rst:  []string{},
		},
		{
			desc: "matching leaf",
			keys: keys[:1],
			rst:  keys[:1],
		},
		{
			desc:  "non matching leaf",
			keys:  keys[:1],
			rst:   []string{},
			start: "13",
		},
		{
			desc: "limited by end",
			keys: keys,
			end:  "125",
			rst:  sorted[:2],
		},
		{
			desc:  "limited by start",
			keys:  keys,
			start: "124",
			rst:   sorted[1:],
		},
		{
			desc:  "start is excluded",
			keys:  keys,
			start: "1234",
			rst:   sorted[1:],
		},
		{
			desc:  "start to end",
			keys:  keys,
			start: "125",
			end:   "1344",
			rst:   sorted[2:3],
		},
		{
			desc:    "reverse",
			keys:    keys,
			rst:     reversed,
			reverse: true,
		},
		{
			desc:    "reverse until",
			keys:    keys,
			start:   "1234",
			rst:     reversed[:4],
			reverse: true,
		},
		{
			desc:    "reverse from",
			keys:    keys,
			end:     "1268",
			rst:     reversed[1:],
			reverse: true,
		},
		{
			desc:    "reverse from until",
			keys:    keys,
			start:   "1235",
			end:     "1268",
			rst:     reversed[1:3],
			reverse: true,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			var tree Tree
			for _, key := range tc.keys {
				tree.Insert([]byte(key), key)
			}
			iter := tree.Iterator([]byte(tc.start), []byte(tc.end))
			if tc.reverse {
				iter = iter.Reverse()
			}
			rst := []string{}
			for iter.Next() {
				rst = append(rst, iter.Value().(string))
			}
			require.Equal(t, tc.rst, rst)
		})
	}
}

func TestIterConcurrentExpansion(t *testing.T) {
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
