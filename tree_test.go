package art

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	seed       = flag.Int64("seed", time.Now().Unix(), "seed for the fuzz test")
	iterations = flag.Int("iter", 1_000_000, "fuzz iterations")
)

const (
	typeInsert = iota + 1
	typeDelete
)

func delOp(key []byte) op {
	return op{
		kv{key: key},
		typeDelete,
	}
}

func insertOp(key []byte, value interface{}) op {
	return op{
		kv{key, value},
		typeInsert,
	}
}

type op struct {
	kv
	typ int
}

type kv struct {
	key   []byte
	value interface{}
}

func TestTreeInsert(t *testing.T) {
	for _, tc := range []struct {
		desc    string
		pretty  string
		inserts []kv
	}{
		{
			desc: "short keys",
			pretty: `inner[]n4[0102]
.leaf[01]
.leaf[02]`,
			inserts: []kv{
				{[]byte{1}, 1},
				{[]byte{2}, 2},
			},
		},
		{
			desc: "long keys",
			pretty: `inner[0100000000000000]n4[00]
.........inner[]n4[0102]
..........leaf[01000000000000000001]
..........leaf[01000000000000000002]`,
			inserts: []kv{
				{[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 1},
				{[]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 2}, 2},
			},
		},
		{
			desc: "normal add child",
			pretty: `inner[]n4[010203]
.leaf[01]
.leaf[02]
.leaf[03]`,
			inserts: []kv{
				{[]byte{3}, 3},
				{[]byte{1}, 1},
				{[]byte{2}, 2},
			},
		},
		{
			desc: "grow",
			pretty: `inner[]n16[0102030405]
.leaf[01]
.leaf[02]
.leaf[03]
.leaf[04]
.leaf[05]`,
			inserts: []kv{
				{[]byte{3}, 3},
				{[]byte{1}, 1},
				{[]byte{4}, 4},
				{[]byte{5}, 5},
				{[]byte{2}, 2},
			},
		},
		{
			desc: "uncompress path",
			pretty: `inner[]n4[0001]
.inner[]n4[0102]
..leaf[0001]
..leaf[0002]
.leaf[0102]`,
			inserts: []kv{
				{[]byte{0, 1}, 1},
				{[]byte{0, 2}, 2},
				{[]byte{1, 2}, 3},
			},
		},
		{
			desc:    "lazy leaf insert",
			pretty:  `leaf[010101]`,
			inserts: []kv{{[]byte{1, 1, 1}, 10}},
		},
		{
			desc: "lazy expansion",
			pretty: `inner[01]n4[0102]
..leaf[010101]
..leaf[010202]`,
			inserts: []kv{
				{[]byte{1, 1, 1}, 10},
				{[]byte{1, 2, 2}, 20},
			},
		},
		{
			desc: "multi inner",
			pretty: `inner[01]n4[010203]
..inner[]n4[0203]
...leaf[01010206]
...leaf[01010304]
..leaf[01020304]
..leaf[010304]`,
			inserts: []kv{
				{[]byte{1, 2, 3, 4}, 20},
				{[]byte{1, 1, 3, 4}, 10},
				{[]byte{1, 1, 2, 6}, 90},
				{[]byte{1, 3, 4}, 320},
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			tree := Tree{}
			for _, insert := range tc.inserts {
				tree.Insert(insert.key, insert.value)
			}
			require.Equal(t, tc.pretty, tree.Pretty())
			for _, insert := range tc.inserts {
				rst, exist := tree.Get(insert.key)
				require.True(t, exist)
				require.Equal(t, insert.value, rst)
			}
		})
	}
}

func TestTreeDelete(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		pretty     string
		operations []op
	}{
		{
			desc:   "collapse inner",
			pretty: `leaf[02]`,
			operations: []op{
				insertOp([]byte{1}, 1),
				insertOp([]byte{2}, 2),
				delOp([]byte{1}),
			},
		},
		{
			desc: "compress path",
			pretty: `inner[01010202]n4[0203]
.....leaf[0101020202]
.....leaf[0101020203]`,
			operations: []op{
				insertOp([]byte{1, 1, 2, 2, 3}, 1),
				insertOp([]byte{1, 1, 1, 3}, 3),
				insertOp([]byte{1, 1, 2, 2, 2}, 2),
				delOp([]byte{1, 1, 1, 3}),
			},
		},
		{
			desc: "shrink",
			pretty: `inner[]n4[01020405]
.leaf[01]
.leaf[02]
.leaf[04]
.leaf[05]`,
			operations: []op{
				insertOp([]byte{1}, 1),
				insertOp([]byte{2}, 2),
				insertOp([]byte{3}, 3),
				insertOp([]byte{4}, 4),
				insertOp([]byte{5}, 5),
				delOp([]byte{3}),
			},
		},
		{
			desc: "normal delete",
			pretty: `inner[]n4[010204]
.leaf[01]
.leaf[02]
.leaf[04]`,
			operations: []op{
				insertOp([]byte{1}, 1),
				insertOp([]byte{2}, 2),
				insertOp([]byte{3}, 3),
				insertOp([]byte{4}, 4),
				delOp([]byte{3}),
			},
		},
		{
			desc:   "delete all",
			pretty: ``,
			operations: []op{
				insertOp([]byte{1}, 1),
				insertOp([]byte{2}, 2),
				delOp([]byte{1}),
				delOp([]byte{2}),
			},
		},
		{
			desc: "delete nonexisting",
			pretty: `inner[]n4[0102]
.leaf[01]
.leaf[02]`,
			operations: []op{
				insertOp([]byte{1}, 1),
				insertOp([]byte{2}, 2),
				delOp([]byte{3}),
			},
		},
		{
			desc: "no compress for long keys",
			pretty: `inner[0100000000000000]n4[02]
.........inner[]n4[0102]
..........leaf[01000000000000000201]
..........leaf[01000000000000000202]`,
			operations: []op{
				insertOp([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 1}, 1),
				insertOp([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 2}, 2),
				insertOp([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}, 3),
				delOp([]byte{1, 0, 0, 0, 0, 0, 0, 0, 1}),
			},
		},
		{
			desc: "reprefix long keys",
			pretty: `inner[0100000000000001]n4[02]
.........inner[]n4[0203]
..........leaf[01000000000000010202]
..........leaf[01000000000000010203]`,
			operations: []op{
				insertOp([]byte{1, 0, 0, 0, 0, 0, 0, 2, 1}, 1),
				insertOp([]byte{1, 0, 0, 0, 0, 0, 0, 1, 2, 2}, 2),
				insertOp([]byte{1, 0, 0, 0, 0, 0, 0, 1, 2, 3}, 3),
				delOp([]byte{1, 0, 0, 0, 0, 0, 0, 2, 1}),
			},
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			tree := Tree{}
			for _, operation := range tc.operations {
				switch operation.typ {
				case typeInsert:
					tree.Insert(operation.key, operation.value)
				case typeDelete:
					tree.Delete(operation.key)
				}
			}
			require.Equal(t, tc.pretty, tree.Pretty())
		})
	}
}

func TestFuzzTree(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Logf("fuzz with seed %v for %v iterations", *seed, *iterations)
	rng := rand.New(rand.NewSource(*seed))

	tree := Tree{}

	keys := [][]byte{}
	vals := []int{}
	for i := 0; i < *iterations; i++ {
		value := rng.Int()
		key := make([]byte, 6)
		_, _ = rng.Read(key)
		tree.Insert(key, value)
		keys = append(keys, key)
		vals = append(vals, value)
	}
	for i := range keys {
		rst, exist := tree.Get(keys[i])
		require.True(t, exist)
		require.Equal(t, vals[i], rst)
	}
	for _, key := range keys {
		tree.Delete([]byte(key))
		_, exist := tree.Get([]byte(key))
		require.False(t, exist)
	}
	fmt.Println(tree.Pretty())
	require.True(t, tree.Empty())
}

func BenchmarkLookups(b *testing.B) {
	rand.Seed(0)
	n := 65_000
	tree := Tree{}
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		rand.Read(key)
		tree.Insert(key, key)
		keys[i] = key
	}

	b.ResetTimer()

	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		index := rand.Intn(n)
		_, _ = tree.Get(keys[index])
	}
}
