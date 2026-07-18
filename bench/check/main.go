package main

// 변환 결과 parquet 검증: 행 수, 집계값, 스키마 출력.

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: check A.parquet")
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	from := "read_parquet('" + strings.ReplaceAll(os.Args[1], "'", "''") + "')"

	var count, sumCol, sumRow int64
	var minH, maxH float64
	var zones int64
	err = db.QueryRow("SELECT count(*), CAST(sum(Col) AS BIGINT), CAST(sum(Row) AS BIGINT), min(Height), max(Height), count(DISTINCT Zone) FROM "+from).
		Scan(&count, &sumCol, &sumRow, &minH, &maxH, &zones)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows=%d sum(Col)=%d sum(Row)=%d Height=[%g,%g] zones=%d\n",
		count, sumCol, sumRow, minH, maxH, zones)

	rows, err := db.Query("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM " + from + ")")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %s %s\n", name, typ)
	}
}
