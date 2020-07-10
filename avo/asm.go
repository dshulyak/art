package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func main() {

	TEXT("search", NOSPLIT, "func(key *byte, nkey *[16]byte) uint16")
	key := Load(Param("key"), GP64())
	nkey := Mem{Base: Load(Param("nkey"), GP64())}

	x0, x1, x2 := XMM(), XMM(), XMM()
	mask := GP32()

	VPXOR(x1, x1, x1)
	VMOVD(Mem{Base: key}, x0)
	VPSHUFB(x1, x0, x0)

	MOVOU(nkey.Offset(0x00), x2)
	PCMPEQB(x2, x0)
	PMOVMSKB(x0, mask)

	Store(mask.As16(), ReturnIndex(0))
	RET()
	Generate()

}
