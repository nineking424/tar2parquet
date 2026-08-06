// Package igzip은 ISA-L(isal_inflate) 기반 gzip 스트리밍 해제 reader를 제공한다.
// klauspost/stdlib gzip 대비 해제 처리량이 크게 높아(Xeon 4.2x, M4 1.9x —
// .scratch 티켓 01 실측) 파이프라인의 기본 해제 경로로 전제한다.
//
// 빌드 전제조건: isa-l 개발 파일 필요.
//   - macOS: brew install isa-l (개발용, 동적 링크)
//   - linux: apt install libisal-dev (배포용, libisal.a 정적 링크 —
//     prebuilt binary가 런타임 라이브러리 없이 동작해야 한다)
package igzip

/*
#cgo darwin CFLAGS: -I/opt/homebrew/opt/isa-l/include
#cgo darwin LDFLAGS: -L/opt/homebrew/opt/isa-l/lib -lisal
#cgo linux LDFLAGS: -l:libisal.a
#include <stdlib.h>
#include <isa-l/igzip_lib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

const (
	inBufSize  = 4 << 20
	outBufSize = 4 << 20
)

// Reader는 gzip 스트림의 스트리밍 해제 io.ReadCloser다.
// klauspost 기본 동작과 동일하게 멀티 멤버(multistream)를 이어서 해제하고,
// 멤버별 gzip CRC/ISIZE 트레일러를 검증한다(ISAL_GZIP).
// cgo 포인터 규칙상 입출력 버퍼와 상태는 C 힙에 두고, 호출자에게는
// out 버퍼에서 복사해 반환한다. 동시 사용 불가(단일 goroutine 전용).
type Reader struct {
	src      io.Reader
	state    *C.struct_inflate_state
	inPtr    unsafe.Pointer
	outPtr   unsafe.Pointer
	in       []byte // C 힙 기반 입력 버퍼
	out      []byte // C 힙 기반 출력 버퍼
	pending  []byte // out 중 아직 호출자에게 전달하지 않은 구간
	filled   int    // in 앞쪽의 미소비 입력 길이
	srcEOF   bool
	finished bool
	err      error
}

// NewReader는 r의 gzip 헤더를 즉시 검증하고 Reader를 반환한다.
// 헤더가 유효하지 않으면(비-gzip 입력 등) 여기서 오류를 돌려준다.
func NewReader(r io.Reader) (*Reader, error) {
	z := &Reader{
		src:    r,
		state:  (*C.struct_inflate_state)(C.malloc(C.sizeof_struct_inflate_state)),
		inPtr:  C.malloc(inBufSize),
		outPtr: C.malloc(outBufSize),
	}
	z.in = unsafe.Slice((*byte)(z.inPtr), inBufSize)
	z.out = unsafe.Slice((*byte)(z.outPtr), outBufSize)
	z.initMember()

	// 헤더 오류를 첫 Read가 아닌 생성 시점에 드러낸다 (stdlib/klauspost와 동일).
	if err := z.round(); err != nil {
		z.Close()
		return nil, err
	}
	return z, nil
}

func (z *Reader) initMember() {
	C.isal_inflate_init(z.state)
	z.state.crc_flag = C.ISAL_GZIP // gzip 헤더 파싱 + CRC/ISIZE 트레일러 검증
}

func (z *Reader) Read(p []byte) (int, error) {
	for len(z.pending) == 0 {
		if z.err != nil {
			return 0, z.err
		}
		if z.finished {
			return 0, io.EOF
		}
		if err := z.round(); err != nil {
			z.err = err
			return 0, err
		}
	}
	n := copy(p, z.pending)
	z.pending = z.pending[n:]
	return n, nil
}

// round는 isal_inflate를 1회 수행해 pending을 채우거나 스트림 종료를 확정한다.
func (z *Reader) round() error {
	if z.filled == 0 && !z.srcEOF {
		if err := z.fill(); err != nil {
			return err
		}
	}

	if z.filled > 0 {
		z.state.next_in = (*C.uint8_t)(z.inPtr)
		z.state.avail_in = C.uint32_t(z.filled)
	}
	z.state.next_out = (*C.uint8_t)(z.outPtr)
	z.state.avail_out = C.uint32_t(outBufSize)

	ret := C.isal_inflate(z.state)
	if ret != C.ISAL_DECOMP_OK {
		return decodeError(ret)
	}

	produced := outBufSize - int(z.state.avail_out)
	z.pending = z.out[:produced]

	// 소비된 입력을 버퍼 앞으로 당겨 다음 라운드에서 이어붙인다.
	remaining := int(z.state.avail_in)
	if remaining > 0 && remaining != z.filled {
		copy(z.in, z.in[z.filled-remaining:z.filled])
	}
	z.filled = remaining

	if z.state.block_state == C.ISAL_BLOCK_FINISH {
		// 멤버 종료. 뒤에 입력이 더 있으면 다음 멤버(multistream),
		// 없으면 스트림 전체 종료.
		if z.filled == 0 && !z.srcEOF {
			if err := z.fill(); err != nil {
				return err
			}
		}
		if z.filled == 0 {
			z.finished = true
			return nil
		}
		z.initMember()
	} else if produced == 0 && z.filled == 0 && z.srcEOF {
		return io.ErrUnexpectedEOF // 마지막 블록 이전에 입력이 끊김
	}
	return nil
}

// fill은 in[filled:]를 src로 채운다. EOF는 srcEOF로만 기록한다.
func (z *Reader) fill() error {
	n, err := io.ReadFull(z.src, z.in[z.filled:])
	z.filled += n
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		z.srcEOF = true
		return nil
	}
	return err
}

// Close는 C 힙 자원을 해제한다. 이후의 Read는 os.ErrClosed를 돌려준다.
func (z *Reader) Close() error {
	if z.state == nil {
		return nil
	}
	C.free(unsafe.Pointer(z.state))
	C.free(z.inPtr)
	C.free(z.outPtr)
	z.state, z.inPtr, z.outPtr = nil, nil, nil
	z.in, z.out, z.pending = nil, nil, nil
	z.err = os.ErrClosed
	return nil
}

func decodeError(ret C.int) error {
	switch ret {
	case C.ISAL_INVALID_WRAPPER:
		return errors.New("igzip: invalid gzip header")
	case C.ISAL_UNSUPPORTED_METHOD:
		return errors.New("igzip: unsupported compression method")
	case C.ISAL_INCORRECT_CHECKSUM:
		return errors.New("igzip: checksum mismatch")
	case C.ISAL_INVALID_BLOCK, C.ISAL_INVALID_SYMBOL, C.ISAL_INVALID_LOOKBACK:
		return fmt.Errorf("igzip: corrupt deflate stream (code %d)", int(ret))
	default:
		return fmt.Errorf("igzip: isal_inflate failed (code %d)", int(ret))
	}
}
