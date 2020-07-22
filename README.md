Concurrent Adaptive Radix Trie
===

https://db.in.tum.de/~leis/papers/ART.pdf
https://15721.courses.cs.cmu.edu/spring2017/papers/08-oltpindexes2/leis-damon2016.pdf
http://sites.computer.org/debull/A19mar/p73.pdf

Notes:

- technically key isn't allowed to be the direct prefix of another key, every string must be null terminated
- library doesn't copy keys or values when they are inserted, if this is required user of the library must case about this
- safety in concurrent environment is achieved with optimistic locks.
  ROWEX-based concurrency is not implemented.