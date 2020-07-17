// +build !amd64

package art

// binary search is slower then 16 elem loop, 23ns > 16ns per op in worst case of scanning whole array
// no reason to use binary search for non-vectorized version
func index(key *byte, nkey *[16]byte) (int, bool) {
	for i := range nkey {
		if nkey[i] == *key {
			return i, true
		}
	}
	return 0, false
}
