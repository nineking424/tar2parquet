package main

// chunkfill.go: FillChunk의 고속 경로 — DuckDB 벡터 메모리에 직접 쓴다.
//
// 프로파일(표준 샘플) 근거: 셀 단위 SetChunkValue 경로는
//   - VARCHAR 셀마다 cgo 호출(duckdb_vector_assign_string_element_len)
//   - 제네릭 setter 디스패치와 projection 맵 조회
//   - 잦은 cgo 전이가 유발하는 스케줄러 wakeup(pthread_cond_signal)
// 으로 총 CPU의 ~15%를 소모한다. 이 경로는 벡터의 data 배열에 typed slice로
// 직접 쓰고, ≤12바이트 문자열은 duckdb_string_t inline 표현으로 cgo 없이 쓴다.
//
// 전제(불변식) — 위반 시 bindFastColumns가 nil을 반환해 기존
// SetChunkValue 폴백 경로로 자동 전환된다:
//  1. duckdb.DataChunk 구조체의 첫 필드는 mapping.DataChunk다(offset 0).
//     드라이버 v2.10504.0 고정 하에 성립하며 rawChunkLayoutOK가 검증한다.
//  2. 본 프로그램의 쿼리는 `COPY (SELECT * FROM tar_csv())`뿐이므로 청크의
//     projection은 항등이다. 컬럼 수·타입 일치를 청크마다 재검증한다.
//     (동일 타입 컬럼만 뒤바뀌는 permutation은 이 검증으로 잡을 수 없다 —
//     쿼리를 SELECT * 외로 바꾸면 이 전제를 재검토할 것.)
//  3. duckdb_string_t는 16바이트 union이고 길이 ≤ 12바이트면 inline 저장,
//     초과분은 별도 힙 포인터를 가진다(duckdb.h 명세).
//  4. validity mask는 uint64 워드 배열이며 row의 유효 비트는
//     mask[row/64]의 (row%64)번째 비트다(duckdb.h 명세). 새 청크는 전 행
//     valid로 시작하므로 invalid 전환만 수행한다.

import (
	"reflect"
	"strconv"
	"unsafe"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/duckdb/duckdb-go/v2/mapping"
)

var rawChunkLayoutOK = func() bool {
	t := reflect.TypeOf(duckdb.DataChunk{})
	return t.NumField() > 0 && t.Field(0).Offset == 0 &&
		t.Field(0).Type == reflect.TypeOf(mapping.DataChunk{})
}()

// stringT는 duckdb_string_t의 inline 표현이다(불변식 3).
type stringT struct {
	length uint32
	data   [12]byte
}

type fastCol struct {
	typ  duckdb.Type
	vec  mapping.Vector // 12바이트 초과 문자열의 cgo 폴백용
	mask []uint64       // validity mask (불변식 4)
	i64  []int64
	f64  []float64
	str  []stringT
}

// bindFastColumns는 output 청크의 벡터들을 직접 쓰기용으로 바인딩한다.
// 청크 모양이 기대와 다르면 nil을 반환한다(SetChunkValue 폴백).
func bindFastColumns(chunk *duckdb.DataChunk, columns []column, cols []fastCol) []fastCol {
	if !rawChunkLayoutOK {
		return nil
	}
	raw := *(*mapping.DataChunk)(unsafe.Pointer(chunk)) // 불변식 1
	if int(mapping.DataChunkGetColumnCount(raw)) != len(columns) {
		return nil
	}
	capacity := duckdb.GetDataChunkCapacity()
	maskWords := (capacity + 63) / 64

	for i, c := range columns {
		vec := mapping.DataChunkGetVector(raw, mapping.IdxT(i))
		lt := mapping.VectorGetColumnType(vec)
		typ := mapping.GetTypeId(lt)
		mapping.DestroyLogicalType(&lt)
		if typ != c.typ {
			return nil
		}
		mapping.VectorEnsureValidityWritable(vec)
		fc := fastCol{
			typ:  c.typ,
			vec:  vec,
			mask: unsafe.Slice((*uint64)(mapping.VectorGetValidity(vec)), maskWords),
		}
		data := mapping.VectorGetData(vec)
		switch c.typ {
		case duckdb.TYPE_BIGINT:
			fc.i64 = unsafe.Slice((*int64)(data), capacity)
		case duckdb.TYPE_DOUBLE:
			fc.f64 = unsafe.Slice((*float64)(data), capacity)
		default: // VARCHAR (inferColumns는 세 타입만 생성한다)
			fc.str = unsafe.Slice((*stringT)(data), capacity)
		}
		cols = append(cols, fc)
	}
	return cols
}

// set은 CSV 필드 하나를 파싱해 벡터에 직접 쓴다.
func (c *fastCol) set(row int, field []byte) error {
	if len(field) == 0 {
		c.mask[row>>6] &^= 1 << (row & 63) // NULL
		return nil
	}
	switch c.typ {
	case duckdb.TYPE_BIGINT:
		v, ok := parseIntFast(field)
		if !ok {
			var err error
			if v, err = strconv.ParseInt(bstr(field), 10, 64); err != nil {
				return err
			}
		}
		c.i64[row] = v
	case duckdb.TYPE_DOUBLE:
		v, ok := parseFloatFast(field)
		if !ok {
			var err error
			if v, err = strconv.ParseFloat(bstr(field), 64); err != nil {
				return err
			}
		}
		c.f64[row] = v
	default:
		if len(field) <= len(stringT{}.data) {
			var s stringT
			s.length = uint32(len(field))
			copy(s.data[:], field)
			c.str[row] = s
		} else {
			mapping.VectorAssignStringElementLen(c.vec, mapping.IdxT(row), field)
		}
	}
	return nil
}

// parseIntFast는 부호 있는 십진 정수의 fast path다. 18자리를 넘거나
// 숫자 외 문자가 있으면 폴백(false). 성공 시 strconv.ParseInt와 값이 같다.
func parseIntFast(b []byte) (int64, bool) {
	i := 0
	neg := false
	if b[0] == '+' || b[0] == '-' {
		neg = b[0] == '-'
		i = 1
	}
	if i == len(b) || len(b)-i > 18 {
		return 0, false
	}
	var n int64
	for ; i < len(b); i++ {
		d := b[i] - '0'
		if d > 9 {
			return 0, false
		}
		n = n*10 + int64(d)
	}
	if neg {
		n = -n
	}
	return n, true
}

// pow10f의 값들은 float64로 정확히 표현된다(10^22까지 가능; 15까지만 사용).
var pow10f = [16]float64{1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7,
	1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15}

// parseFloatFast는 지수 표기가 없는 십진 소수의 fast path다. 총 자릿수 ≤ 15
// 이면 가수(< 10^15 < 2^53)와 10^frac이 모두 float64로 정확하므로, 단일
// 나눗셈의 IEEE 반올림 결과가 strconv.ParseFloat와 비트 단위로 동일하다
// (Clinger fast path). 그 외 입력(지수, 16자리 이상, inf/NaN)은 폴백(false).
func parseFloatFast(b []byte) (float64, bool) {
	i := 0
	neg := false
	if b[0] == '+' || b[0] == '-' {
		neg = b[0] == '-'
		i = 1
	}
	var mant uint64
	digits, frac := 0, 0
	seenDot := false
	for ; i < len(b); i++ {
		c := b[i]
		if c == '.' {
			if seenDot {
				return 0, false
			}
			seenDot = true
			continue
		}
		d := c - '0'
		if d > 9 {
			return 0, false
		}
		mant = mant*10 + uint64(d)
		digits++
		if seenDot {
			frac++
		}
	}
	if digits == 0 || digits > 15 {
		return 0, false
	}
	v := float64(mant) / pow10f[frac]
	if neg {
		v = -v
	}
	return v, true
}
