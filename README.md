Concurrent Adaptive Radix Trie
===

- [ART](https://db.in.tum.de/~leis/papers/ART.pdf)
- [Concurrent ART: OLC and ROWEX](https://15721.courses.cs.cmu.edu/spring2017/papers/08-oltpindexes2/leis-damon2016.pdf)
- [OLC: Optimistic Lock Coupling](http://sites.computer.org/debull/A19mar/p73.pdf)

Considerations:

- key isn't allowed to be the direct prefix of another key, every string must be null terminated.
- library doesn't copy keys when they are inserted, caller must care about this.
- safety in concurrent environment is achieved with optimistic locks.
  ROWEX-based concurrency is not implemented.
- Non-negligible amount of time is spent in GC. Memory ballast improves, but doesn't solve, the issue.  
