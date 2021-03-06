package art

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
			desc: "uncompress path 2",
			pretty: `inner[01]n4[0102]
..inner[02]n4[0304]
....leaf[01010203]
....leaf[01010204]
..leaf[01020304]`,
			inserts: []kv{
				{[]byte{1, 1, 2, 4}, 1},
				{[]byte{1, 1, 2, 3}, 2},
				{[]byte{1, 2, 3, 4}, 3},
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
			require.Equal(t, tc.pretty, tree.testView())
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
			require.Equal(t, tc.pretty, tree.testView())
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
	require.True(t, tree.Empty())
}

func BenchmarkLookups(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000, 1_000_000, 10_000_000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			benchmarkLookups(b, size)
		})
	}
}

func benchmarkLookups(b *testing.B, n int) {
	rand.Seed(0)
	tree := Tree{}
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 24)
		rand.Read(key)
		tree.Insert(key, key)
		keys[i] = key
	}

	b.ResetTimer()
	b.SetBytes(1)
	idx := 0
	for i := 0; i < b.N; i++ {
		if idx >= len(keys) {
			idx = 0
		}
		_, _ = tree.Get(keys[idx])
		idx++
	}
}

var ballast []byte

func BenchmarkInserts(b *testing.B) {
	ballast = make([]byte, 1<<30)
	rand.Seed(0)
	n := 65_000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		rand.Read(key)
		keys[i] = key
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var tree Tree
		for _, key := range keys {
			tree.Insert(key, key)
		}
	}
}

func TestTreeConcurrentInsert(t *testing.T) {
	var tree Tree
	keys := []string{
		"aabd",
		"aabe",
		"abcd",
		"aedd",
		"aqdd",
	}
	updates := 10
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(key string) {
			for i := 1; i <= updates; i++ {
				tree.Insert([]byte(key), i)
			}
			wg.Done()
		}(key)
	}
	wg.Wait()

	for _, key := range keys {
		rst, exist := tree.Get([]byte(key))
		assert.True(t, exist, "key '%v' should exist", key)
		assert.Equal(t, updates, rst)
	}
}

func TestTreeConcurrentDelete(t *testing.T) {
	cnt := 100_000
	factor := 8
	keys := [][]byte{}
	var tree Tree
	for i := 0; i < cnt; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		tree.Insert(key, key)
		keys = append(keys, key)
	}
	var wg sync.WaitGroup
	keyc := make(chan []byte, factor)
	for i := 0; i < factor; i++ {
		wg.Add(1)
		go func() {
			for key := range keyc {
				tree.Delete(key)
			}
			wg.Done()
		}()
	}
	for _, key := range keys {
		keyc <- key
	}
	close(keyc)
	wg.Wait()
	require.True(t, tree.Empty())
}

func TestTreeInsertDeleteConcurrent(t *testing.T) {
	var (
		wg   sync.WaitGroup
		cnt  = 100
		keys = [][]byte{}

		tree = Tree{}
	)
	for i := 0; i < cnt; i++ {
		key := make([]byte, 8)
		rand.Read(key)
		keys = append(keys, key)
	}

	wg.Add(2)
	go func() {
		for i := 0; i < cnt; i++ {
			key := keys[rand.Intn(len(keys))]
			tree.Insert(key, key)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < cnt; i++ {
			key := keys[rand.Intn(len(keys))]
			tree.Delete(key)
		}
		wg.Done()
	}()
	wg.Wait()

	for _, key := range keys {
		tree.Delete(key)
	}
	for _, key := range keys {
		_, found := tree.Get(key)
		require.False(t, found)
	}

	require.True(t, tree.Empty())
}

func randomKey(rng *rand.Rand) [16]byte {
	b := [16]byte{}
	binary.LittleEndian.PutUint32(b[:], rng.Uint32())
	binary.LittleEndian.PutUint32(b[4:], rng.Uint32())
	binary.BigEndian.PutUint64(b[8:], math.MaxUint64)
	return b
}

func BenchmarkGetInsert(b *testing.B) {
	value := 123
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			tree := Tree{}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := randomKey(rng)
					if rng.Float32() < readFrac {
						_, _ = tree.Get(key[:])
					} else {
						tree.Insert(key[:], value)
					}
				}
			})
		})
	}
}

func BenchmarkGetInsertSyncMap(b *testing.B) {
	value := 123
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			tree := sync.Map{}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := randomKey(rng)
					if rng.Float32() < readFrac {
						_, _ = tree.Load(key)
					} else {
						tree.Store(key, value)
					}
				}
			})
		})
	}
}
