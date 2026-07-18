package main

// tar.gz 안의 CSV들을 단일 패스 스트리밍으로 읽어 하나의 Parquet으로 변환한다.
//
// 아키텍처:
//
//	tar.gz ─(prefetch)─> gzip ─> tar ─> 헤더 제거 ─> 행 경계 블록 분할 ─> 유한 채널
//	                                                                        │
//	     DuckDB COPY (SELECT * FROM tar_csv()) TO parquet  <── FillChunk(멀티스레드 파싱/적재)
//
// DuckDB의 read_csv + pipe 조합은 duckdb-go v2.10504에서 바인딩 시점 스키마가
// placeholder로 잡히고 0행을 반환하는 문제가 있어(조용한 데이터 소실),
// Table UDF(ParallelChunkTableSource)로 Go가 직접 데이터를 공급한다.
// 이 방식은 파이프와 달리 CSV 파싱과 Parquet 인코딩이 전 코어로 병렬화된다.
//
// 전제(REQUIREMENTS.md §12): 모든 CSV는 동일한 스키마와 헤더를 가진다.
// 블록 분할이 행 경계('\n')를 전제로 하므로 quoted field 안의 개행은 지원하지 않는다.

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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/klauspost/compress/gzip"
)

const (
	prefetchBlockSize = 4 << 20 // NFS 등 고지연 입력의 read-ahead 단위
	prefetchDepth     = 2
	rowBlockSize      = 2 << 20 // FillChunk 스레드에 전달되는 행 블록 크기
	feedDepth         = 4
	maxHeaderLen      = 1 << 20
	sampleRows        = 1024 // 미지 컬럼 타입 추론에 사용하는 행 수
)

// 알려진 컬럼명 → 타입. 헤더에 없는 컬럼은 자연히 무시되고,
// 여기에 없는 컬럼은 첫 블록 샘플에서 BIGINT/DOUBLE/VARCHAR 중 추론한다.
var knownColumnTypes = map[string]duckdb.Type{
	"Col":    duckdb.TYPE_BIGINT,
	"Row":    duckdb.TYPE_BIGINT,
	"ChipX":  duckdb.TYPE_DOUBLE,
	"ChipY":  duckdb.TYPE_DOUBLE,
	"WaferX": duckdb.TYPE_DOUBLE,
	"WaferY": duckdb.TYPE_DOUBLE,
	"Height": duckdb.TYPE_DOUBLE,
	"Zone":   duckdb.TYPE_VARCHAR,
}

var errAborted = errors.New("conversion aborted")

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

type column struct {
	name string
	typ  duckdb.Type
}

type schemaResult struct {
	columns []column
	err     error
}

// feed는 tar 스트리머가 만든 행 블록을 DuckDB 스레드들에 전달한다.
type feed struct {
	blocks chan []byte
	done   chan struct{} // 닫히면 producer 중단 (쿼리 조기 실패 시)
	err    error         // producer 오류; close(blocks) 이전에만 쓴다
}

func convert(src, dst string) error {
	ctx := context.Background()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("duckdb conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SET preserve_insertion_order = false"); err != nil {
		return fmt.Errorf("configure duckdb: %w", err)
	}

	// TAR2PARQUET_THREADS: 병렬도 상한 (기본: 코어 수). 벤치마크/코어 제한용.
	threads := runtime.NumCPU()
	if v, err := strconv.Atoi(os.Getenv("TAR2PARQUET_THREADS")); err == nil && v > 0 {
		threads = v
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET threads = %d", threads)); err != nil {
		return fmt.Errorf("configure duckdb: %w", err)
	}

	fd := &feed{
		blocks: make(chan []byte, feedDepth),
		done:   make(chan struct{}),
	}
	schemaCh := make(chan schemaResult, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fd.err = streamTarGZ(src, fd, schemaCh)
		close(fd.blocks)
	}()

	schema := <-schemaCh
	if schema.err != nil {
		close(fd.done)
		wg.Wait()
		return fmt.Errorf("stream: %w", schema.err)
	}

	source := &csvSource{fd: fd, columns: schema.columns, maxThreads: threads}
	udf := duckdb.ParallelChunkTableFunction{
		Config: duckdb.TableFunctionConfig{},
		BindArguments: func(map[string]any, ...any) (duckdb.ParallelChunkTableSource, error) {
			return source, nil
		},
	}
	if err := duckdb.RegisterTableUDF(conn, "tar_csv", udf); err != nil {
		close(fd.done)
		wg.Wait()
		return fmt.Errorf("register udf: %w", err)
	}

	tmp := dst + ".tmp"
	query := fmt.Sprintf(
		"COPY (SELECT * FROM tar_csv()) TO '%s' (FORMAT parquet, COMPRESSION zstd)",
		escapeSQLString(tmp))

	_, queryErr := conn.ExecContext(ctx, query)

	// 쿼리가 조기 종료한 경우 채널 send에 블록된 producer를 깨운다.
	close(fd.done)
	wg.Wait()

	if queryErr != nil {
		os.Remove(tmp)
		return fmt.Errorf("duckdb: %w", queryErr)
	}
	if fd.err != nil {
		os.Remove(tmp)
		return fmt.Errorf("stream: %w", fd.err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
}

// ---- DuckDB table UDF source ----

type csvSource struct {
	fd         *feed
	columns    []column
	maxThreads int
}

func (s *csvSource) ColumnInfos() []duckdb.ColumnInfo {
	infos := make([]duckdb.ColumnInfo, len(s.columns))
	for i, c := range s.columns {
		t, err := duckdb.NewTypeInfo(c.typ)
		if err != nil {
			panic(err) // BIGINT/DOUBLE/VARCHAR는 실패할 수 없다
		}
		infos[i] = duckdb.ColumnInfo{Name: c.name, T: t}
	}
	return infos
}

func (s *csvSource) Init() duckdb.ParallelTableSourceInfo {
	// 주의: 0을 넘기면 드라이버가 DuckDB에 max_threads=0을 그대로 전달해
	// 단일 스레드 스캔이 된다. 반드시 양수를 지정한다.
	return duckdb.ParallelTableSourceInfo{MaxThreads: s.maxThreads}
}

func (s *csvSource) NewLocalState() any {
	return &fillState{}
}

func (s *csvSource) Cardinality() *duckdb.CardinalityInfo {
	return nil
}

type fillState struct {
	block  []byte
	off    int
	fields [][]byte
}

// FillChunk은 DuckDB의 여러 스레드에서 동시에 호출된다.
// 각 스레드는 행 블록 단위로 작업을 가져가므로 상호 간섭 없이 파싱한다.
func (s *csvSource) FillChunk(ls any, chunk duckdb.DataChunk) error {
	state := ls.(*fillState)
	capacity := duckdb.GetDataChunkCapacity()
	row := 0

	for row < capacity {
		if state.off >= len(state.block) {
			if row > 0 {
				break // 부분 chunk 반환; 다음 블록은 다음 호출에서
			}
			block, ok := <-s.fd.blocks
			if !ok {
				if s.fd.err != nil {
					return s.fd.err
				}
				return chunk.SetSize(0) // 정상 종료
			}
			state.block, state.off = block, 0
		}

		line := nextLine(state)
		if len(line) == 0 {
			continue
		}

		fields, err := splitFields(line, state.fields[:0])
		state.fields = fields
		if err != nil {
			return fmt.Errorf("csv parse: %w (row: %.100s)", err, line)
		}
		if len(fields) != len(s.columns) {
			return fmt.Errorf("csv parse: row has %d fields, want %d (row: %.100s)",
				len(fields), len(s.columns), line)
		}

		for i, col := range s.columns {
			if err := setField(chunk, i, row, col.typ, fields[i]); err != nil {
				return fmt.Errorf("column %s: %w (row: %.100s)", col.name, err, line)
			}
		}
		row++
	}

	return chunk.SetSize(row)
}

func nextLine(state *fillState) []byte {
	rest := state.block[state.off:]
	nl := bytes.IndexByte(rest, '\n')

	var line []byte
	if nl < 0 {
		line = rest
		state.off = len(state.block)
	} else {
		line = rest[:nl]
		state.off += nl + 1
	}

	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line
}

func setField(chunk duckdb.DataChunk, col, row int, typ duckdb.Type, field []byte) error {
	if len(field) == 0 {
		return chunk.SetValue(col, row, nil) // NULL
	}

	switch typ {
	case duckdb.TYPE_BIGINT:
		v, err := strconv.ParseInt(bstr(field), 10, 64)
		if err != nil {
			return err
		}
		return duckdb.SetChunkValue(chunk, col, row, v)
	case duckdb.TYPE_DOUBLE:
		v, err := strconv.ParseFloat(bstr(field), 64)
		if err != nil {
			return err
		}
		return duckdb.SetChunkValue(chunk, col, row, v)
	default:
		return duckdb.SetChunkValue(chunk, col, row, string(field))
	}
}

// bstr은 파싱 hot path에서 필드별 복사를 피한다. 반환 문자열은 보관하지 않는다.
func bstr(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// splitFields는 한 행을 필드로 나눈다. 최소한의 RFC4180:
// quoted field는 콤마와 "" escape를 지원한다(개행 미지원).
func splitFields(line []byte, fields [][]byte) ([][]byte, error) {
	for {
		if len(line) == 0 {
			return append(fields, nil), nil
		}

		if line[0] == '"' {
			i := 1
			escaped := false
			for {
				j := bytes.IndexByte(line[i:], '"')
				if j < 0 {
					return fields, errors.New("unterminated quoted field")
				}
				i += j + 1
				if i < len(line) && line[i] == '"' {
					escaped = true
					i++
					continue
				}
				break
			}

			field := line[1 : i-1]
			if escaped {
				field = bytes.ReplaceAll(field, []byte(`""`), []byte(`"`))
			}
			fields = append(fields, field)

			if i == len(line) {
				return fields, nil
			}
			if line[i] != ',' {
				return fields, errors.New("unexpected character after quoted field")
			}
			line = line[i+1:]
			continue
		}

		j := bytes.IndexByte(line, ',')
		if j < 0 {
			return append(fields, line), nil
		}
		fields = append(fields, line[:j])
		line = line[j+1:]
	}
}

// ---- tar.gz streaming ----

func streamTarGZ(src string, fd *feed, schemaCh chan<- schemaResult) error {
	var names []string
	schemaSent := false

	fail := func(err error) error {
		if !schemaSent {
			schemaCh <- schemaResult{err: err}
			schemaSent = true
		}
		return err
	}

	// 첫 블록에서 스키마를 확정해 보낸 뒤 채널로 전달한다.
	emit := func(block []byte) error {
		if !schemaSent {
			schemaCh <- schemaResult{columns: inferColumns(names, block)}
			schemaSent = true
		}
		select {
		case fd.blocks <- block:
			return nil
		case <-fd.done:
			return errAborted
		}
	}

	f, err := os.Open(src)
	if err != nil {
		return fail(err)
	}
	defer f.Close()

	done := make(chan struct{})
	defer close(done)

	gz, err := gzip.NewReader(newPrefetchReader(f, prefetchBlockSize, prefetchDepth, done))
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
			if !schemaSent {
				// 데이터 행이 하나도 없는 archive: 헤더만으로 스키마 확정.
				schemaCh <- schemaResult{columns: inferColumns(names, nil)}
				schemaSent = true
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

// inferColumns는 알려진 컬럼은 고정 타입을, 미지의 컬럼은 샘플 행에서
// BIGINT → DOUBLE → VARCHAR 순으로 추론한 타입을 부여한다.
func inferColumns(names []string, sample []byte) []column {
	columns := make([]column, len(names))
	var unknown []int
	for i, n := range names {
		if t, ok := knownColumnTypes[n]; ok {
			columns[i] = column{name: n, typ: t}
		} else {
			columns[i] = column{name: n, typ: duckdb.TYPE_BIGINT}
			unknown = append(unknown, i)
		}
	}
	if len(unknown) == 0 {
		return columns
	}

	canInt := make(map[int]bool, len(unknown))
	canFloat := make(map[int]bool, len(unknown))
	for _, i := range unknown {
		canInt[i], canFloat[i] = true, true
	}

	var fields [][]byte
	rows := 0
	for len(sample) > 0 && rows < sampleRows {
		nl := bytes.IndexByte(sample, '\n')
		var line []byte
		if nl < 0 {
			line, sample = sample, nil
		} else {
			line, sample = sample[:nl], sample[nl+1:]
		}
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		var err error
		if fields, err = splitFields(line, fields[:0]); err != nil || len(fields) != len(names) {
			continue // 실제 오류는 FillChunk에서 보고된다
		}
		rows++

		for _, i := range unknown {
			f := fields[i]
			if len(f) == 0 {
				continue
			}
			if canInt[i] {
				if _, err := strconv.ParseInt(bstr(f), 10, 64); err != nil {
					canInt[i] = false
				}
			}
			if !canInt[i] && canFloat[i] {
				if _, err := strconv.ParseFloat(bstr(f), 64); err != nil {
					canFloat[i] = false
				}
			}
		}
	}

	for _, i := range unknown {
		switch {
		case canInt[i]:
			columns[i].typ = duckdb.TYPE_BIGINT
		case canFloat[i]:
			columns[i].typ = duckdb.TYPE_DOUBLE
		default:
			columns[i].typ = duckdb.TYPE_VARCHAR
		}
	}
	return columns
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
