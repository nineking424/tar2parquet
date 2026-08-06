package main

// tar.gz 안의 CSV들을 단일 패스 스트리밍으로 읽어 하나의 Parquet으로 변환한다.
//
// 아키텍처:
//
//	tar.gz ─(prefetch)─> igzip(ISA-L) ─> tar ─> 헤더 제거 ─> 행 경계 블록 분할 ─> 유한 채널
//	                                                                        │
//	     DuckDB COPY (SELECT * FROM tar_csv()) TO parquet  <── FillChunk(멀티스레드 파싱/적재)
//
// 파일 = 모듈: producer.go(스트리밍·블록 생산), consumer.go(UDF 파싱·적재),
// chunkfill.go(적재 fast path), schema.go(스키마 확정), igzip/(해제).
// 이 파일에는 파이프라인 조립(convert)과 두 모듈을 잇는 동시성 규약(feed)만 남는다.
//
// DuckDB의 read_csv + pipe 조합은 duckdb-go v2.10504에서 바인딩 시점 스키마가
// placeholder로 잡히고 0행을 반환하는 문제가 있어(조용한 데이터 소실),
// Table UDF(ParallelChunkTableSource)로 Go가 직접 데이터를 공급한다.
// 이 방식은 파이프와 달리 CSV 파싱과 Parquet 인코딩이 전 코어로 병렬화된다.
//
// 전제(REQUIREMENTS.md §12): 모든 CSV는 동일한 스키마와 헤더를 가진다.
// 블록 분할이 행 경계('\n')를 전제로 하므로 quoted field 안의 개행은 지원하지 않는다.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

const feedDepth = 4

var errAborted = errors.New("conversion aborted")

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s A.tar.gz", filepath.Base(os.Args[0]))
	}

	src := os.Args[1]
	dst := outputPath(src)

	// TAR2PARQUET_CPUPROFILE: pprof CPU 프로파일 출력 경로 (성능 분석용).
	if path := os.Getenv("TAR2PARQUET_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	if err := convert(src, dst); err != nil {
		log.Fatal(err)
	}

	log.Printf("created: %s", dst)
}

// feed는 producer(스트리밍 goroutine)가 만든 행 블록을 DuckDB 스레드들에
// 전달한다. 채널과 오류 필드는 이 타입의 메서드로만 다룬다. 불변식:
//
//  1. emit·finish는 producer goroutine 하나만 호출한다.
//  2. finish는 오류를 기록한 뒤 채널을 닫는다 — next가 닫힘을 관측하면
//     (channel close의 happens-before) 오류 읽기는 추가 동기화 없이 안전하다.
//  3. abort는 소비 측이 쿼리 조기 종료 시 호출한다(멱등). 이후의 emit은
//     errAborted를 돌려 채널 send에 블록된 producer를 깨운다.
type feed struct {
	blocks    chan []byte
	done      chan struct{}
	abortOnce sync.Once
	err       error
}

func newFeed() *feed {
	return &feed{
		blocks: make(chan []byte, feedDepth),
		done:   make(chan struct{}),
	}
}

// emit은 행 블록을 소비 측에 전달한다. abort 이후에는 errAborted.
func (f *feed) emit(block []byte) error {
	select {
	case f.blocks <- block:
		return nil
	case <-f.done:
		return errAborted
	}
}

// finish는 producer의 종료를 알린다. err는 next와 error로 전파된다.
func (f *feed) finish(err error) {
	f.err = err
	close(f.blocks)
}

// next는 다음 행 블록을 돌려준다. (nil, nil)은 정상 종료,
// (nil, err)는 producer 오류다.
func (f *feed) next() ([]byte, error) {
	block, ok := <-f.blocks
	if !ok {
		return nil, f.err
	}
	return block, nil
}

// abort는 producer 중단을 요청한다 (쿼리 조기 실패 시). 멱등.
func (f *feed) abort() {
	f.abortOnce.Do(func() { close(f.done) })
}

// error는 producer의 최종 오류를 돌려준다. finish 이후에만 호출한다
// (convert에서는 wg.Wait 뒤).
func (f *feed) error() error {
	return f.err
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

	fd := newFeed()
	schema := newSchemaPromise()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := streamTarGZ(src, fd, schema)
		// 구조적 백스톱: streamTarGZ의 어떤 반환 경로에서도 아래 wait가
		// 영원히 블록되지 않는다 (이미 확정됐다면 no-op).
		schema.resolve(func() schemaResult {
			if err != nil {
				return schemaResult{err: err}
			}
			return schemaResult{err: errors.New("stream ended without schema")}
		})
		fd.finish(err)
	}()

	res := schema.wait()
	if res.err != nil {
		fd.abort()
		wg.Wait()
		return fmt.Errorf("stream: %w", res.err)
	}

	source := &csvSource{fd: fd, columns: res.columns, maxThreads: threads}
	udf := duckdb.ParallelChunkTableFunction{
		Config: duckdb.TableFunctionConfig{},
		BindArguments: func(map[string]any, ...any) (duckdb.ParallelChunkTableSource, error) {
			return source, nil
		},
	}
	if err := duckdb.RegisterTableUDF(conn, "tar_csv", udf); err != nil {
		fd.abort()
		wg.Wait()
		return fmt.Errorf("register udf: %w", err)
	}

	tmp := dst + ".tmp"
	query := fmt.Sprintf(
		"COPY (SELECT * FROM tar_csv()) TO '%s' (FORMAT parquet, COMPRESSION zstd)",
		escapeSQLString(tmp))

	_, queryErr := conn.ExecContext(ctx, query)

	// 쿼리가 조기 종료한 경우 채널 send에 블록된 producer를 깨운다.
	fd.abort()
	wg.Wait()

	if queryErr != nil {
		os.Remove(tmp)
		return fmt.Errorf("duckdb: %w", queryErr)
	}
	if err := fd.error(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("stream: %w", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("finalize: %w", err)
	}

	return nil
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
