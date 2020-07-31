Concurrent Adaptive Radix Trie
===

References
---

- [ART](https://db.in.tum.de/~leis/papers/ART.pdf)
- [Concurrent ART: OLC and ROWEX](https://15721.courses.cs.cmu.edu/spring2017/papers/08-oltpindexes2/leis-damon2016.pdf)
- [OLC: Optimistic Lock Coupling](http://sites.computer.org/debull/A19mar/p73.pdf)

API
---

```go

tree := Tree{}
tree.Insert([]byte{1,2,3}, 123})
tree.Insert([]byte{1,4,5}, 145})

value, found := tree.Get([]byte{1,2,3})
if !found || value.(int) != 123 {
   panic("value must be eq to 123")
}

tree.Delete([]byte{1,2,3})

_, found = tree.Get([]byte{1,2,3})
if found {
   panic("value is found after it was deleted")
}

tree.Insert([]byte{1,4,3}, 143)
iter := tree.Iterator(nil, nil)
for iter.Next() {
    fmt.Println(iter.Value())
}

iter := tree.Iterator(nil, nil).Reverse()
for iter.Next() {
    fmt.Println(iter.Value())
}
```

Notes
---

- key isn't allowed to be the direct prefix of another key. If arbitrary string are used as keys - caller must ensure that every string is null terminated.
- library doesn't copy keys when they are inserted, caller must take care about this if such safety is requried.
- safety in concurrent environment is achieved with optimistic locks.
  ROWEX-based concurrency is not implemented.
- Non-negligible amount of time is spent in GC. Memory ballast improves, but doesn't solve, the problem.