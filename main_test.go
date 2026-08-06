package main

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type tarFile struct {
	name string
	data string
}

func writeTarGZ(t *testing.T, path string, files []tarFile) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)

	for _, tf := range files {
		if err := tw.WriteHeader(&tar.Header{
			Name:     tf.name,
			Mode:     0o644,
			Size:     int64(len(tf.data)),
			Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(tf.data)); err != nil {
			t.Fatal(err)
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConvert(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "A.tar.gz")
	dst := filepath.Join(dir, "A.parquet")

	// Col/Row/ChipX/Zone: 알려진 컬럼. Foo(float)/Bar(int): 타입 추론 대상.
	// Zone "01"은 추론이면 BIGINT가 되므로 VARCHAR 강제를 검증한다.
	// A-4의 Zone은 12바이트 초과 → string_t inline이 아닌 힙 경로를 검증한다.
	writeTarGZ(t, src, []tarFile{
		{"README.txt", "not a csv\n"},
		{"A-1.csv", "Col,Row,ChipX,Zone,Foo,Bar\n1,2,1.5,01,0.1,10\n3,4,2.5,\"z,2\",0.2,20\n"},
		{"A-2.csv", "Col,Row,ChipX,Zone,Foo,Bar\n5,6,3.5,01,0.3,30"}, // 개행 없이 끝남
		{"A-3.csv", "Col,Row,ChipX,Zone,Foo,Bar\n7,8,4.5,02,,40\n"},  // Foo 빈 값 → NULL
		{"A-4.csv", "Col,Row,ChipX,Zone,Foo,Bar\n9,10,5.5,zone-longer-than-12-bytes,0.4,50\n"},
	})

	if err := convert(src, dst); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	from := "read_parquet('" + strings.ReplaceAll(dst, "'", "''") + "')"

	var (
		count    int
		sumCol   int64
		sumBar   int64
		countFoo int
		minZone  string
	)
	if err := db.QueryRow(
		"SELECT count(*), CAST(sum(Col) AS BIGINT), CAST(sum(Bar) AS BIGINT), count(Foo), min(Zone) FROM "+from,
	).Scan(&count, &sumCol, &sumBar, &countFoo, &minZone); err != nil {
		t.Fatal(err)
	}
	if count != 5 || sumCol != 25 || sumBar != 150 || countFoo != 4 || minZone != "01" {
		t.Errorf("got count=%d sum(Col)=%d sum(Bar)=%d count(Foo)=%d min(Zone)=%q, want 5, 25, 150, 4, \"01\"",
			count, sumCol, sumBar, countFoo, minZone)
	}

	var quotedZone string
	if err := db.QueryRow("SELECT Zone FROM " + from + " WHERE Col = 3").Scan(&quotedZone); err != nil {
		t.Fatal(err)
	}
	if quotedZone != "z,2" {
		t.Errorf("quoted Zone = %q, want \"z,2\"", quotedZone)
	}

	var longZone string
	if err := db.QueryRow("SELECT Zone FROM " + from + " WHERE Col = 9").Scan(&longZone); err != nil {
		t.Fatal(err)
	}
	if longZone != "zone-longer-than-12-bytes" {
		t.Errorf("long Zone = %q, want \"zone-longer-than-12-bytes\"", longZone)
	}

	rows, err := db.Query("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM " + from + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	got := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		got[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"Col":   "BIGINT",
		"Row":   "BIGINT",
		"ChipX": "DOUBLE",
		"Zone":  "VARCHAR",
		"Foo":   "DOUBLE",
		"Bar":   "BIGINT",
	}
	for name, typ := range want {
		if got[name] != typ {
			t.Errorf("column %s: got type %q, want %q", name, got[name], typ)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d columns (%v), want %d", len(got), got, len(want))
	}
}

func TestConvertNoCSV(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "A.tar.gz")
	dst := filepath.Join(dir, "A.parquet")

	writeTarGZ(t, src, []tarFile{{"README.txt", "no csv here\n"}})

	if err := convert(src, dst); err == nil {
		t.Fatal("expected error for archive without csv")
	}
	for _, p := range []string{dst, dst + ".tmp"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should not exist", p)
		}
	}
}

func TestConvertCorruptInput(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "A.tar.gz")
	dst := filepath.Join(dir, "A.parquet")

	if err := os.WriteFile(src, []byte("this is not gzip"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := convert(src, dst); err == nil {
		t.Fatal("expected error for corrupt input")
	}
}

// gzip 스트림이 첫 행 블록(~2MiB) 방출 전에 끊기면 스키마가 미확정인 채로
// producer가 종료된다 — 이 경로가 오류로 끝나야 한다. (schemaPromise 백스톱
// 이전에는 convert가 스키마 대기에서 영원히 블록되는 데드락이었다.)
func TestConvertTruncatedMidStream(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "A.tar.gz")
	dst := filepath.Join(dir, "A.parquet")

	rng := rand.New(rand.NewSource(7))
	var sb strings.Builder
	sb.WriteString("Col,Row\n")
	for sb.Len() < 3<<20 {
		sb.WriteString(strconv.Itoa(rng.Intn(1 << 30)))
		sb.WriteByte(',')
		sb.WriteString(strconv.Itoa(rng.Intn(1 << 30)))
		sb.WriteByte('\n')
	}
	writeTarGZ(t, src, []tarFile{{name: "A-1.csv", data: sb.String()}})

	info, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(src, info.Size()/4); err != nil {
		t.Fatal(err)
	}

	if err := convert(src, dst); err == nil {
		t.Fatal("expected error for truncated stream")
	}
}

// TestFastPathLayout: 고속 경로의 불변식 1(드라이버 구조체 레이아웃)이
// 깨지면 — 예: 드라이버 업그레이드 — 여기서 먼저 드러나야 한다.
// 폴백 덕에 동작은 유지되지만, 성능 저하가 조용히 지나가면 안 된다.
func TestFastPathLayout(t *testing.T) {
	if !rawChunkLayoutOK {
		t.Error("duckdb.DataChunk layout changed; fast path disabled (silent perf regression)")
	}
}

func TestParseIntFast(t *testing.T) {
	for _, s := range []string{
		"0", "1", "42", "-7", "+7", "9223372036854775807", "-0",
		"999999999999999999", "0042", "12x", "x", "-", "+", "", " 1", "1 ",
		"1.5", "1e3", "123456789012345678901",
	} {
		if s == "" {
			continue // 빈 필드는 NULL로 처리되어 파서에 오지 않는다
		}
		got, ok := parseIntFast([]byte(s))
		want, err := strconv.ParseInt(s, 10, 64)
		if ok && err != nil {
			t.Errorf("parseIntFast(%q) accepted invalid input: got %d", s, got)
		}
		if ok && err == nil && got != want {
			t.Errorf("parseIntFast(%q) = %d, want %d", s, got, want)
		}
	}
}

func TestParseFloatFast(t *testing.T) {
	for _, s := range []string{
		"0", "1", "-1", "+1", "0.5", "-0.0", "137.000", "0.00332",
		"189.7", "1256.6472", "0.33200", "1.", ".5", "-.5",
		"999999999999999", "0.000000000001",
		"1e3", "1E3", "inf", "NaN", "1.2.3", "x", "-", "1234567890123456",
	} {
		got, ok := parseFloatFast([]byte(s))
		want, err := strconv.ParseFloat(s, 64)
		if ok && err != nil {
			t.Errorf("parseFloatFast(%q) accepted invalid input: got %g", s, got)
		}
		if ok && err == nil && math.Float64bits(got) != math.Float64bits(want) {
			t.Errorf("parseFloatFast(%q) = %v (bits %x), want %v (bits %x)",
				s, got, math.Float64bits(got), want, math.Float64bits(want))
		}
	}

	// 생성기 형식의 값들을 무작위로 strconv와 비트 단위 대조한다.
	rng := rand.New(rand.NewSource(1))
	for range 10000 {
		s := strconv.FormatFloat(rng.Float64()*float64(rng.Intn(2000)), 'f', rng.Intn(6), 64)
		got, ok := parseFloatFast([]byte(s))
		if !ok {
			t.Fatalf("parseFloatFast(%q) unexpectedly fell back", s)
		}
		want, _ := strconv.ParseFloat(s, 64)
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("parseFloatFast(%q) = %x, want %x", s, math.Float64bits(got), math.Float64bits(want))
		}
	}
}

func TestConvertMalformedRow(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "A.tar.gz")
	dst := filepath.Join(dir, "A.parquet")

	// 필드 수가 맞지 않는 행은 §11에 따라 실패해야 한다.
	writeTarGZ(t, src, []tarFile{
		{"A-1.csv", "Col,Row\n1,2\n3,4,5\n"},
	})

	if err := convert(src, dst); err == nil {
		t.Fatal("expected error for malformed row")
	}
	for _, p := range []string{dst, dst + ".tmp"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should not exist", p)
		}
	}
}
