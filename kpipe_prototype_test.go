//go:build prototype

package main

// 단계2 검증용 프로토타입 (배포 바이너리에 포함되지 않음).
//
// 검증 질문:
//  1. DuckDB가 UNION ALL로 결합된 K개의 pipe scan을 동시에 소비하는가?
//  2. 단일 producer가 파일 단위 round-robin으로 유한 queue에 분배할 때
//     교착 없이 완주하는가? (순차 스케줄링이면 queue가 가득 차 교착 → timeout)
//  3. k=1 대비 k>1의 처리 시간 개선이 있는가?
//
// 실행: go test -tags prototype -run TestKPipe -v

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	protoFileCount   = 32
	protoRowsPerFile = 200_000
	protoQueueDepth  = 4
)

func synthCSVFile(fileIdx int) []byte {
	var b strings.Builder
	b.Grow(protoRowsPerFile * 48)
	for i := 0; i < protoRowsPerFile; i++ {
		fmt.Fprintf(&b, "%d,%d,%.3f,%.3f,zone%d\n",
			fileIdx, i, float64(i)*1.5, float64(i)*2.5, i%8)
	}
	return []byte(b.String())
}

func runKPipe(t *testing.T, k int) time.Duration {
	t.Helper()

	dir := t.TempDir()
	out := filepath.Join(dir, "out.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("SET preserve_insertion_order = false"); err != nil {
		t.Fatal(err)
	}

	type stream struct {
		queue chan []byte
		r     *os.File
		w     *os.File
	}

	streams := make([]*stream, k)
	var writers sync.WaitGroup
	for i := range streams {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		s := &stream{queue: make(chan []byte, protoQueueDepth), r: r, w: w}
		streams[i] = s

		writers.Add(1)
		go func() {
			defer writers.Done()
			defer s.w.Close()
			for block := range s.queue {
				if _, err := s.w.Write(block); err != nil {
					return
				}
			}
		}()
	}

	// producer: tar 순차 읽기를 모사 — 파일 단위 round-robin 분배.
	go func() {
		for i := 0; i < protoFileCount; i++ {
			streams[i%k].queue <- synthCSVFile(i)
		}
		for _, s := range streams {
			close(s.queue)
		}
	}()

	scans := make([]string, k)
	for i, s := range streams {
		scans[i] = fmt.Sprintf(
			"SELECT * FROM read_csv('/dev/fd/%d', header=false,"+
				" names=['Col','Row','ChipX','ChipY','Zone'],"+
				" types={'Col':'BIGINT','Row':'BIGINT','ChipX':'DOUBLE','ChipY':'DOUBLE','Zone':'VARCHAR'})",
			s.r.Fd())
	}
	query := fmt.Sprintf(
		"COPY (%s) TO '%s' (FORMAT parquet, COMPRESSION zstd)",
		strings.Join(scans, " UNION ALL "), out)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	start := time.Now()
	_, queryErr := db.ExecContext(ctx, query)
	elapsed := time.Since(start)

	for _, s := range streams {
		s.r.Close()
	}
	writers.Wait()

	if queryErr != nil {
		t.Fatalf("query failed (순차 스케줄링에 의한 교착이면 timeout): %v", queryErr)
	}

	var count int64
	if err := db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", out),
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if want := int64(protoFileCount * protoRowsPerFile); count != want {
		t.Fatalf("row count %d, want %d", count, want)
	}

	return elapsed
}

func TestKPipe(t *testing.T) {
	for _, k := range []int{1, 4} {
		k := k
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			elapsed := runKPipe(t, k)
			t.Logf("k=%d: %d rows in %v", k, protoFileCount*protoRowsPerFile, elapsed)
		})
	}
}
