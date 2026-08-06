package main

// ISA-L igzip 해제 단독 처리량 측정 (bench/gunzip의 klauspost 대조군).
// 스트리밍 API(isal_inflate)만 사용 — 전체 버퍼 적재는 메모리 제약상 금지.
//
// 빌드: isa-l 설치 필요 (macOS: brew install isa-l / 데비안: apt install libisal-dev).

/*
#cgo darwin CFLAGS: -I/opt/homebrew/opt/isa-l/include
#cgo darwin LDFLAGS: -L/opt/homebrew/opt/isa-l/lib
#cgo LDFLAGS: -lisal
#include <stdlib.h>
#include <isa-l/igzip_lib.h>
*/
import "C"

import (
	"bufio"
	"flag"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
	"unsafe"
)

const (
	inBufSize  = 4 << 20
	outBufSize = 4 << 20
)

func main() {
	sum := flag.Bool("sum", false, "해제 결과의 CRC32(IEEE)를 함께 출력 (정합성 대조용)")
	flag.Parse()

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var h hash.Hash32
	if *sum {
		h = crc32.NewIEEE()
	}

	start := time.Now()
	n, err := inflateStream(bufio.NewReaderSize(f, inBufSize), h)
	if err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("decompressed %d bytes in %v (%.0f MB/s)\n",
		n, elapsed, float64(n)/elapsed.Seconds()/1e6)
	if h != nil {
		fmt.Printf("crc32 %08x\n", h.Sum32())
	}
}

// inflateStream은 gzip 스트림 r을 isal_inflate로 해제하며 총 출력 바이트를
// 반환한다. h가 nil이 아니면 출력 바이트를 해시에 반영한다.
// 멀티 멤버 gzip도 klauspost 기본 동작(multistream)과 동일하게 이어서 해제한다.
func inflateStream(r io.Reader, h hash.Hash32) (int64, error) {
	// cgo 포인터 규칙상 state에 Go 메모리 주소를 담을 수 없어 C 힙을 쓴다.
	inPtr := C.malloc(inBufSize)
	outPtr := C.malloc(outBufSize)
	defer C.free(inPtr)
	defer C.free(outPtr)
	inBuf := unsafe.Slice((*byte)(inPtr), inBufSize)
	outBuf := unsafe.Slice((*byte)(outPtr), outBufSize)

	var state C.struct_inflate_state
	initMember := func() {
		C.isal_inflate_init(&state)
		state.crc_flag = C.ISAL_GZIP // gzip 헤더 파싱 + CRC/ISIZE 트레일러 검증
	}
	initMember()

	var total int64
	var filled int // inBuf 내 미소비 입력 길이
	eof := false

	for {
		if filled == 0 && !eof {
			n, err := io.ReadFull(r, inBuf)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				eof = true
			} else if err != nil {
				return total, err
			}
			filled = n
		}
		if filled > 0 {
			state.next_in = (*C.uint8_t)(unsafe.Pointer(&inBuf[0]))
			state.avail_in = C.uint32_t(filled)
		}
		state.next_out = (*C.uint8_t)(unsafe.Pointer(&outBuf[0]))
		state.avail_out = C.uint32_t(len(outBuf))

		ret := C.isal_inflate(&state)
		if ret != C.ISAL_DECOMP_OK {
			return total, fmt.Errorf("isal_inflate: error %d", int(ret))
		}

		produced := len(outBuf) - int(state.avail_out)
		if produced > 0 {
			total += int64(produced)
			if h != nil {
				h.Write(outBuf[:produced])
			}
		}
		// 소비된 입력을 버퍼 앞으로 당겨 다음 라운드에서 이어붙인다.
		remaining := int(state.avail_in)
		if remaining > 0 && remaining != filled {
			consumed := filled - remaining
			copy(inBuf, inBuf[consumed:filled])
		}
		filled = remaining

		if state.block_state == C.ISAL_BLOCK_FINISH {
			if filled == 0 && eof {
				return total, nil
			}
			initMember() // 다음 gzip 멤버 (multistream)
		} else if filled == 0 && eof && produced == 0 {
			return total, fmt.Errorf("truncated gzip stream")
		}
	}
}
