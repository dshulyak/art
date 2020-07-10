.PHONY: asm
asm: search_avx_amd64.s

search_avx_amd64.s: avo/asm.go
	go run ./avo/asm.go -out search_avx_amd64.s
