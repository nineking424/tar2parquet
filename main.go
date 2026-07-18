package main

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/klauspost/compress/gzip"
)

const (
	bufferSize    = 4 << 20 // 4 MiB
	prefetchDepth = 2
	maxHeaderLen  = 1 << 20 // 1 MiB
)

// 알려진 컬럼명 → DuckDB 타입. 헤더에 없는 컬럼은 자연히 무시되고,
// 여기에 없는 컬럼은 BIGINT/DOUBLE/VARCHAR 중 자동 추론된다.
var knownColumnTypes = map[string]string{
	"Col":    "BIGINT",
	"Row":    "BIGINT",
	"ChipX":  "DOUBLE",
	"ChipY":  "DOUBLE",
	"WaferX": "DOUBLE",
	"WaferY": "DOUBLE",
	"Height": "DOUBLE",
	"Zone":   "VARCHAR",
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s A.tar.gz", filepath.Base(os.Args[0]))
	}

	src := os.Args[1]
	dst := outputPath(src)

	if err := convert(src, dst); err != nil {
		log.Fatal(err)
	}

	log.Printf("created: %s", dst)
}

type header struct {
	columns []string
	err     error
}

func convert(src, dst string) error {
	ctx := context.Background()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "SET preserve_insertion_order = false"); err != nil {
		return fmt.Errorf("configure duckdb: %w", err)
	}

	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create pipe: %w", err)
	}

	fdPath := fmt.Sprintf("/dev/fd/%d", r.Fd())

	headerCh := make(chan header, 1)

	var (
		wg        sync.WaitGroup
		streamErr error
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer w.Close()
		streamErr = streamTarGZ(src, w, headerCh)
	}()

	h := <-headerCh
	if h.err != nil {
		r.Close()
		wg.Wait()
		return fmt.Errorf("stream: %w", h.err)
	}

	tmp := dst + ".tmp"
	_, queryErr := db.ExecContext(ctx, buildQuery(fdPath, tmp, h.columns))

	// DuckDB가 조기 종료한 경우 파이프 write에 블록된 writer를 깨운다.
	r.Close()
	wg.Wait()

	if queryErr != nil {
		os.Remove(tmp)
		return fmt.Errorf("duckdb: %w", queryErr)
	}
	if streamErr != nil {
		os.Remove(tmp)
		return fmt.Errorf("stream: %w", streamErr)
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
}

func buildQuery(src, dst string, columns []string) string {
	var types []string
	for _, c := range columns {
		if t, ok := knownColumnTypes[c]; ok {
			types = append(types, fmt.Sprintf("'%s': '%s'", escapeSQLString(c), t))
		}
	}

	typesArg := ""
	if len(types) > 0 {
		typesArg = fmt.Sprintf(",\n\t\ttypes = {%s}", strings.Join(types, ", "))
	}

	return fmt.Sprintf(`
COPY (
	SELECT *
	FROM read_csv(
		'%s',
		header = true,
		auto_detect = true,
		auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR']%s
	)
)
TO '%s'
(
	FORMAT parquet,
	COMPRESSION zstd
)
`, escapeSQLString(src), typesArg, escapeSQLString(dst))
}

func streamTarGZ(src string, dst io.Writer, headerCh chan<- header) error {
	sent := false
	fail := func(err error) error {
		if !sent {
			headerCh <- header{err: err}
			sent = true
		}
		return err
	}

	f, err := os.Open(src)
	if err != nil {
		return fail(err)
	}
	defer f.Close()

	done := make(chan struct{})
	defer close(done)

	gz, err := gzip.NewReader(newPrefetchReader(f, bufferSize, prefetchDepth, done))
	if err != nil {
		return fail(fmt.Errorf("gzip: %w", err))
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	buf := make([]byte, bufferSize)
	first := true

	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			if first {
				return fail(errors.New("no csv file in archive"))
			}
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

		if first {
			columns, err := parseHeaderLine(line)
			if err != nil {
				return fail(fmt.Errorf("%s header: %w", h.Name, err))
			}

			headerCh <- header{columns: columns}
			sent = true

			if _, err := dst.Write(line); err != nil {
				return fmt.Errorf("%s header: %w", h.Name, err)
			}
			if line[len(line)-1] != '\n' {
				if _, err := dst.Write([]byte{'\n'}); err != nil {
					return fmt.Errorf("%s header: %w", h.Name, err)
				}
			}

			first = false
		}

		if err := copyBody(tr, dst, buf); err != nil {
			return fmt.Errorf("%s data: %w", h.Name, err)
		}
	}
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

// copyBody는 entry의 나머지 데이터를 복사하고, 파일이 개행 없이 끝나면
// 다음 파일의 첫 row와 병합되지 않도록 개행을 보정한다.
func copyBody(r io.Reader, w io.Writer, buf []byte) error {
	last := byte('\n')

	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			last = buf[n-1]
		}
		if errors.Is(err, io.EOF) {
			if last != '\n' {
				if _, werr := w.Write([]byte{'\n'}); werr != nil {
					return werr
				}
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
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

func outputPath(src string) string {
	switch {
	case strings.HasSuffix(src, ".tar.gz"):
		return strings.TrimSuffix(src, ".tar.gz") + ".parquet"
	case strings.HasSuffix(src, ".tgz"):
		return strings.TrimSuffix(src, ".tgz") + ".parquet"
	default:
		return src + ".parquet"
	}
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
