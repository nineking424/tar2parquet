package main

// consumer.go: DuckDB table UDF 소비 모듈 — feed의 행 블록을 파싱해 청크에 적재한다.
//
// 모듈 interface: csvSource(duckdb.ParallelChunkTableSource 구현) 하나.
// convert가 &csvSource{fd, columns, maxThreads}로 조립해 tar_csv UDF로 등록한다.
//
// 동시성 계약: FillChunk은 DuckDB의 여러 스레드에서 동시에 호출된다.
// 스레드별 상태는 fillState(NewLocalState)로 격리되고, 스레드 간 접점은
// fd.next()뿐이다. 각 스레드는 행 블록 단위로 작업을 가져가므로 블록 내부
// 파싱은 상호 간섭이 없다.
//
// 적재는 chunkfill.go의 fast path(벡터 직접 쓰기)를 우선 사용하고, 전제
// 위반 시 셀 단위 SetChunkValue 경로로 자동 폴백한다.
//
// 오류 모드: 파싱 오류(quote 미종결, 필드 수 불일치, 타입 변환 실패)는
// 문제 행의 앞 100바이트를 담아 반환되고, DuckDB 쿼리 실패로 전파된다.
// producer 오류는 fd.next를 거쳐 그대로 반환된다.

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"unsafe"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

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
	fast   []fastCol
}

func (s *csvSource) FillChunk(ls any, chunk duckdb.DataChunk) error {
	state := ls.(*fillState)
	capacity := duckdb.GetDataChunkCapacity()
	state.fast = bindFastColumns(&chunk, s.columns, state.fast[:0])
	row := 0

	for row < capacity {
		if state.off >= len(state.block) {
			if row > 0 {
				break // 부분 chunk 반환; 다음 블록은 다음 호출에서
			}
			block, err := s.fd.next()
			if err != nil {
				return err
			}
			if block == nil {
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

		if fast := state.fast; fast != nil {
			for i := range fast {
				if err := fast[i].set(row, fields[i]); err != nil {
					return fmt.Errorf("column %s: %w (row: %.100s)", s.columns[i].name, err, line)
				}
			}
		} else {
			for i, col := range s.columns {
				if err := setField(chunk, i, row, col.typ, fields[i]); err != nil {
					return fmt.Errorf("column %s: %w (row: %.100s)", col.name, err, line)
				}
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
