package main

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"os"
	"path/filepath"
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
	writeTarGZ(t, src, []tarFile{
		{"README.txt", "not a csv\n"},
		{"A-1.csv", "Col,Row,ChipX,Zone,Foo,Bar\n1,2,1.5,01,0.1,10\n3,4,2.5,\"z,2\",0.2,20\n"},
		{"A-2.csv", "Col,Row,ChipX,Zone,Foo,Bar\n5,6,3.5,01,0.3,30"}, // 개행 없이 끝남
		{"A-3.csv", "Col,Row,ChipX,Zone,Foo,Bar\n7,8,4.5,02,,40\n"},  // Foo 빈 값 → NULL
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
	if count != 4 || sumCol != 16 || sumBar != 100 || countFoo != 3 || minZone != "01" {
		t.Errorf("got count=%d sum(Col)=%d sum(Bar)=%d count(Foo)=%d min(Zone)=%q, want 4, 16, 100, 3, \"01\"",
			count, sumCol, sumBar, countFoo, minZone)
	}

	var quotedZone string
	if err := db.QueryRow("SELECT Zone FROM " + from + " WHERE Col = 3").Scan(&quotedZone); err != nil {
		t.Fatal(err)
	}
	if quotedZone != "z,2" {
		t.Errorf("quoted Zone = %q, want \"z,2\"", quotedZone)
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
