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

	writeTarGZ(t, src, []tarFile{
		{"README.txt", "not a csv\n"},
		{"A-1.csv", "Col,Row,ChipX,Zone,Foo\n1,2,1.5,01,0.1\n3,4,2.5,02,0.2\n"},
		{"A-2.csv", "Col,Row,ChipX,Zone,Foo\n5,6,3.5,01,0.3"}, // 개행 없이 끝남
		{"A-3.csv", "Col,Row,ChipX,Zone,Foo\n7,8,4.5,02,0.4\n"},
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
		count   int
		sumCol  int64
		minZone string
	)
	if err := db.QueryRow(
		"SELECT count(*), CAST(sum(Col) AS BIGINT), min(Zone) FROM " + from,
	).Scan(&count, &sumCol, &minZone); err != nil {
		t.Fatal(err)
	}
	if count != 4 || sumCol != 16 || minZone != "01" {
		t.Errorf("got count=%d sum(Col)=%d min(Zone)=%q, want 4, 16, \"01\"", count, sumCol, minZone)
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

	// Zone은 "01" 같은 숫자형 문자열이라 자동 추론이면 BIGINT가 되지만,
	// 알려진 컬럼 타입 강제(types)로 VARCHAR가 유지되어야 한다.
	want := map[string]string{
		"Col":   "BIGINT",
		"Row":   "BIGINT",
		"ChipX": "DOUBLE",
		"Zone":  "VARCHAR",
		"Foo":   "DOUBLE",
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
