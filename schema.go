package main

// schema.go: 스키마 확정 모듈 — 컬럼 타입 결정과 확정의 1회 전달.
//
// 모듈 interface:
//   - inferColumns(names, sample) []column: 알려진 컬럼(knownColumnTypes)은
//     고정 타입을, 미지의 컬럼은 sample(첫 행 블록, 최대 sampleRows행)에서
//     BIGINT → DOUBLE → VARCHAR 순으로 추론한 타입을 부여한다. sample이
//     nil이면(데이터 행 없음) 미지 컬럼은 BIGINT다. 파싱 불가 행은 추론에서
//     건너뛴다 — 실제 오류 보고는 FillChunk의 몫이다.
//   - schemaPromise: 스키마 확정을 정확히 한 번 전달한다. resolve는 멱등 —
//     첫 호출만 채택되고, 인자 함수는 채택될 때만 평가된다(추론 비용 회피).
//     wait는 resolve까지 블록하므로, producer의 모든 종료 경로에서 resolve가
//     보장돼야 한다(convert의 백스톱 resolve 참조).

import (
	"bytes"
	"strconv"
	"sync"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

const sampleRows = 1024 // 미지 컬럼 타입 추론에 사용하는 행 수

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

type column struct {
	name string
	typ  duckdb.Type
}

type schemaResult struct {
	columns []column
	err     error
}

type schemaPromise struct {
	once sync.Once
	ch   chan schemaResult
}

func newSchemaPromise() *schemaPromise {
	return &schemaPromise{ch: make(chan schemaResult, 1)}
}

func (p *schemaPromise) resolve(f func() schemaResult) {
	p.once.Do(func() { p.ch <- f() })
}

func (p *schemaPromise) wait() schemaResult {
	return <-p.ch
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
