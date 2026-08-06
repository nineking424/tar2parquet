package igzip

import (
	"bytes"
	"compress/gzip"
	"io"
	"math/rand"
	"testing"
)

// makeData는 압축 가능한 결정적 데이터를 만든다 (CSV 유사 반복 + 난수 혼합).
func makeData(t *testing.T, size int) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(42))
	data := make([]byte, 0, size)
	for len(data) < size {
		data = append(data, []byte("col1,col2,col3\n")...)
		for i := 0; i < 16; i++ {
			data = append(data, byte('0'+rng.Intn(10)))
		}
		data = append(data, '\n')
	}
	return data[:size]
}

func gzipCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// 출력 버퍼(4MiB)보다 큰 입력으로 다중 라운드 + Read 창 관리를 검증한다.
func TestRoundTrip(t *testing.T) {
	want := makeData(t, 10<<20)
	z, err := NewReader(bytes.NewReader(gzipCompress(t, want)))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()

	got, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

// klauspost 기본 동작과 같은 멀티 멤버(multistream) 연속 해제.
func TestMultistream(t *testing.T) {
	a, b := makeData(t, 1<<20), makeData(t, 100)
	stream := append(gzipCompress(t, a), gzipCompress(t, b)...)

	z, err := NewReader(bytes.NewReader(stream))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()

	got, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append(a, b...)) {
		t.Fatalf("multistream mismatch: got %d bytes, want %d", len(got), len(a)+len(b))
	}
}

func TestInvalidHeader(t *testing.T) {
	if _, err := NewReader(bytes.NewReader([]byte("this is not gzip"))); err == nil {
		t.Fatal("expected error for non-gzip input")
	}
}

func TestEmptyInput(t *testing.T) {
	if _, err := NewReader(bytes.NewReader(nil)); err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestTruncated(t *testing.T) {
	full := gzipCompress(t, makeData(t, 1<<20))
	z, err := NewReader(bytes.NewReader(full[:len(full)/2]))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()

	if _, err := io.ReadAll(z); err == nil {
		t.Fatal("expected error for truncated stream")
	}
}

func TestReadAfterClose(t *testing.T) {
	z, err := NewReader(bytes.NewReader(gzipCompress(t, makeData(t, 100))))
	if err != nil {
		t.Fatal(err)
	}
	z.Close()

	buf := make([]byte, 10)
	if _, err := z.Read(buf); err == nil {
		t.Fatal("expected error after Close")
	}
}
