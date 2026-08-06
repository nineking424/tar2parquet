package main

// producer.go: 스트리밍 producer 모듈 — tar.gz를 읽어 행 경계 블록을 만들어낸다.
//
// 모듈 interface: streamTarGZ(src, fd, schema) error 하나. prefetch(NFS 등
// 고지연 입력의 read-ahead), igzip 해제, tar 순회, CSV 헤더 제거, 행 경계
// 블록 분할을 전부 이 파일 안에 숨긴다.
//
// 계약(호출자가 알아야 할 전부):
//   - producer goroutine 하나에서 호출한다. 블록은 fd.emit으로 내보내며
//     feed의 동시성 규약(main.go)을 따른다. fd.finish는 호출자 책임이다.
//   - schema는 첫 데이터 블록에서 resolve된다(데이터 행이 없는 archive는
//     EOF 시점에 헤더만으로). 열기/해제/헤더 오류 경로는 fail을 거쳐
//     resolve를 보장하지만, 데이터 스트리밍 중 오류(emit 중단 포함) 경로는
//     resolve 없이 반환한다 — 호출자의 백스톱 resolve(convert 참조)가
//     schema.wait의 영원한 블록을 막는 전제다.
//   - 오류 모드: 반환 오류는 원인별로 래핑된다(gzip/tar/파일별 헤더·데이터).
//     소비 측이 fd.abort로 중단하면 errAborted가 반환될 수 있다.

import (
	"archive/tar"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"tar2parquet/igzip"
)

const (
	prefetchBlockSize = 4 << 20 // NFS 등 고지연 입력의 read-ahead 단위
	prefetchDepth     = 2
	rowBlockSize      = 2 << 20 // FillChunk 스레드에 전달되는 행 블록 크기
	maxHeaderLen      = 1 << 20
)

func streamTarGZ(src string, fd *feed, schema *schemaPromise) error {
	var names []string

	fail := func(err error) error {
		schema.resolve(func() schemaResult { return schemaResult{err: err} })
		return err
	}

	// 첫 블록에서 스키마를 확정해 전달한다 (resolve 멱등 — 이후 블록은 no-op).
	emit := func(block []byte) error {
		schema.resolve(func() schemaResult {
			return schemaResult{columns: inferColumns(names, block)}
		})
		return fd.emit(block)
	}

	f, err := os.Open(src)
	if err != nil {
		return fail(err)
	}
	defer f.Close()

	done := make(chan struct{})
	defer close(done)

	gz, err := igzip.NewReader(newPrefetchReader(f, prefetchBlockSize, prefetchDepth, done))
	if err != nil {
		return fail(fmt.Errorf("gzip: %w", err))
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	bw := &blockWriter{emit: emit}

	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			if names == nil {
				return fail(errors.New("no csv file in archive"))
			}
			if err := bw.flush(); err != nil {
				return err
			}
			// 데이터 행이 하나도 없는 archive: 헤더만으로 스키마 확정 (멱등).
			schema.resolve(func() schemaResult {
				return schemaResult{columns: inferColumns(names, nil)}
			})
			return nil
		}
		if err != nil {
			return fail(fmt.Errorf("tar: %w", err))
		}

		if h.Typeflag != tar.TypeReg || !strings.HasSuffix(h.Name, ".csv") {
			continue
		}

		line, err := readHeaderLine(tr)
		if err != nil {
			return fail(fmt.Errorf("%s header: %w", h.Name, err))
		}

		if names == nil {
			if names, err = parseHeaderLine(line); err != nil {
				return fail(fmt.Errorf("%s header: %w", h.Name, err))
			}
		}

		if err := bw.readFrom(tr); err != nil {
			return fmt.Errorf("%s data: %w", h.Name, err)
		}
		bw.endFile()
	}
}

// blockWriter는 CSV body 바이트를 모아 행 경계('\n')에서 잘라 블록으로 내보낸다.
// readFrom이 reader에서 내부 버퍼로 직접 읽으므로 중간 복사가 없다.
type blockWriter struct {
	emit func([]byte) error
	buf  []byte
}

const minReadSpace = 64 << 10

func (w *blockWriter) readFrom(r io.Reader) error {
	for {
		if cap(w.buf)-len(w.buf) < minReadSpace {
			if err := w.cut(); err != nil {
				return err
			}
		}

		n, err := r.Read(w.buf[len(w.buf):cap(w.buf)])
		w.buf = w.buf[:len(w.buf)+n]

		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// cut은 버퍼가 찼을 때 행 경계에서 잘라 블록을 내보내고,
// 남은 partial row를 새 버퍼로 옮긴다. '\n'이 없으면(블록보다 긴 행) 버퍼를 키운다.
func (w *blockWriter) cut() error {
	i := bytes.LastIndexByte(w.buf, '\n')
	if i < 0 {
		grown := make([]byte, len(w.buf), max(cap(w.buf)*2, rowBlockSize+minReadSpace))
		copy(grown, w.buf)
		w.buf = grown
		return nil
	}

	block := w.buf[:i+1]
	rest := w.buf[i+1:]
	w.buf = make([]byte, len(rest), rowBlockSize+minReadSpace)
	copy(w.buf, rest)

	return w.emit(block)
}

// endFile은 파일이 개행 없이 끝났을 때 다음 파일의 첫 row와
// 병합되지 않도록 개행을 보정한다.
func (w *blockWriter) endFile() {
	if len(w.buf) > 0 && w.buf[len(w.buf)-1] != '\n' {
		w.buf = append(w.buf, '\n')
	}
}

func (w *blockWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	block := w.buf
	w.buf = nil
	return w.emit(block)
}

// readHeaderLine은 현재 entry의 첫 line을 개행 포함으로 읽는다.
// entry 데이터를 초과 소비하지 않도록 1 byte씩 읽는다(헤더는 짧아 비용 무시 가능).
func readHeaderLine(r io.Reader) ([]byte, error) {
	line := make([]byte, 0, 256)
	var b [1]byte

	for {
		n, err := r.Read(b[:])
		if n > 0 {
			line = append(line, b[0])
			if b[0] == '\n' {
				return line, nil
			}
			if len(line) > maxHeaderLen {
				return nil, errors.New("header line too long")
			}
		}
		if errors.Is(err, io.EOF) {
			if len(line) == 0 {
				return nil, errors.New("empty csv file")
			}
			return line, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func parseHeaderLine(line []byte) ([]string, error) {
	line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
	return csv.NewReader(bytes.NewReader(line)).Read()
}

// prefetchReader는 별도 goroutine이 source(NFS 등 고지연 I/O)를 미리 읽어,
// 디스크 read와 압축 해제가 겹치도록 한다.
type prefetchReader struct {
	blocks <-chan []byte
	errc   <-chan error
	cur    []byte
	err    error
}

func newPrefetchReader(r io.Reader, blockSize, depth int, done <-chan struct{}) *prefetchReader {
	blocks := make(chan []byte, depth)
	errc := make(chan error, 1)

	go func() {
		defer close(blocks)
		for {
			buf := make([]byte, blockSize)
			n, err := io.ReadFull(r, buf)
			if n > 0 {
				select {
				case blocks <- buf[:n]:
				case <-done:
					return
				}
			}
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) {
					err = io.EOF
				}
				errc <- err
				return
			}
		}
	}()

	return &prefetchReader{blocks: blocks, errc: errc}
}

func (p *prefetchReader) Read(b []byte) (int, error) {
	if p.err != nil {
		return 0, p.err
	}

	for len(p.cur) == 0 {
		block, ok := <-p.blocks
		if !ok {
			p.err = <-p.errc
			return 0, p.err
		}
		p.cur = block
	}

	n := copy(b, p.cur)
	p.cur = p.cur[n:]
	return n, nil
}
