package art

import "math/bits"

func index(key *byte, nkey *[16]byte) (int, bool) {
	bitfield := search(key, nkey)
	if bitfield == 0 {
		return 0, false
	}
	return bits.TrailingZeros16(bitfield), true
}

func search(key *byte, nkey *[16]byte) uint16
