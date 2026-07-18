package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

const expCSV = "Col,Row,ChipX,Zone,Foo\n1,2,1.5,01,0.1\n3,4,2.5,02,0.2\n"

func expRun(t *testing.T, path, opts string) error {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("SELECT count(*) FROM read_csv('%s', %s)", path, opts))
	return err
}

func TestExpVariants(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "a.csv")
	if err := os.WriteFile(file, []byte(expCSV), 0o644); err != nil {
		t.Fatal(err)
	}

	variants := []struct {
		name string
		opts string
	}{
		{"file/types+candidates", "header=true, auto_detect=true, auto_type_candidates=['BIGINT','DOUBLE','VARCHAR'], types={'Col':'BIGINT','Zone':'VARCHAR'}"},
		{"file/types", "header=true, auto_detect=true, types={'Col':'BIGINT','Zone':'VARCHAR'}"},
		{"file/candidates", "header=true, auto_detect=true, auto_type_candidates=['BIGINT','DOUBLE','VARCHAR']"},
		{"file/names+types", "header=true, auto_detect=true, auto_type_candidates=['BIGINT','DOUBLE','VARCHAR'], names=['Col','Row','ChipX','Zone','Foo'], types={'Col':'BIGINT','Zone':'VARCHAR'}"},
	}

	for _, v := range variants {
		v := v
		t.Run(v.name, func(t *testing.T) {
			if err := expRun(t, file, v.opts); err != nil {
				t.Errorf("failed: %v", err)
			}
		})
	}

	for _, v := range variants {
		v := v
		t.Run("pipe/"+v.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			go func() {
				defer w.Close()
				w.Write([]byte(expCSV))
			}()

			if err := expRun(t, fmt.Sprintf("/dev/fd/%d", r.Fd()), v.opts); err != nil {
				t.Errorf("failed: %v", err)
			}
		})
	}
}

func TestExpPipeRawExec(t *testing.T) {
	// database/sql의 prepare/execute 이중 바인딩이 파이프를 소모하는지 검증:
	// driver raw connection으로 직접 실행.
	dir := t.TempDir()
	out := filepath.Join(dir, "out.parquet")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	go func() {
		defer w.Close()
		w.Write([]byte(expCSV))
	}()

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	conn, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	execer := conn.(driver.ExecerContext)

	q := fmt.Sprintf(
		"COPY (SELECT * FROM read_csv('/dev/fd/%d', header=true, auto_detect=true, auto_type_candidates=['BIGINT','DOUBLE','VARCHAR'])) TO '%s' (FORMAT parquet)",
		r.Fd(), out)
	if _, err := execer.ExecContext(context.Background(), q, nil); err != nil {
		t.Fatalf("raw exec failed: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", out))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		t.Logf("column: %s %s", name, typ)
	}

	var count int64
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", out)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	t.Logf("rows: %d (want 2)", count)
	if count != 2 {
		t.Errorf("row count = %d, want 2", count)
	}
}

func TestExpPipeIntegrity(t *testing.T) {
	// 파이프 + SELECT * + COPY TO parquet 경로가 컬럼명/행 수를 보존하는지 검증.
	dir := t.TempDir()
	out := filepath.Join(dir, "out.parquet")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	go func() {
		defer w.Close()
		w.Write([]byte(expCSV))
	}()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	q := fmt.Sprintf(
		"COPY (SELECT * FROM read_csv('/dev/fd/%d', header=true, auto_detect=true, auto_type_candidates=['BIGINT','DOUBLE','VARCHAR'])) TO '%s' (FORMAT parquet)",
		r.Fd(), out)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("copy failed: %v", err)
	}

	rows, err := db.Query(fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", out))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		t.Logf("column: %s %s", name, typ)
	}

	var count int64
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", out)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	t.Logf("rows: %d (want 2)", count)
	if count != 2 {
		t.Errorf("row count = %d, want 2", count)
	}
}

func TestExpPipeSchema(t *testing.T) {
	// 파이프 + 스니핑에서 헤더 컬럼명이 결과 스키마에 유지되는지,
	// 명명 컬럼 참조(CAST projection)가 바인딩되는지 확인.
	t.Run("named-projection", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		go func() {
			defer w.Close()
			w.Write([]byte(expCSV))
		}()

		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		var col int64
		var zone string
		q := fmt.Sprintf(
			"SELECT CAST(max(Col) AS BIGINT), max(CAST(Zone AS VARCHAR)) FROM read_csv('/dev/fd/%d', header=true, auto_detect=true, auto_type_candidates=['BIGINT','DOUBLE','VARCHAR'])",
			r.Fd())
		if err := db.QueryRow(q).Scan(&col, &zone); err != nil {
			t.Fatalf("failed: %v", err)
		}
		t.Logf("max(Col)=%d max(Zone as varchar)=%q", col, zone)
	})

	// 단계2 조합: header=false + names + types (파이프)
	t.Run("headerless-names-types", func(t *testing.T) {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		go func() {
			defer w.Close()
			w.Write([]byte("1,2,1.5,01,0.1\n3,4,2.5,02,0.2\n")) // 헤더 없음
		}()

		if err := expRun(t, fmt.Sprintf("/dev/fd/%d", r.Fd()),
			"header=false, names=['Col','Row','ChipX','Zone','Foo'], types={'Col':'BIGINT','Zone':'VARCHAR'}, auto_type_candidates=['BIGINT','DOUBLE','VARCHAR']"); err != nil {
			t.Errorf("failed: %v", err)
		}
	})
}
