package main

// gzip 해제 단독 처리량 측정 (변환 파이프라인의 이론적 상한).

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/klauspost/compress/gzip"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	start := time.Now()
	gz, err := gzip.NewReader(bufio.NewReaderSize(f, 4<<20))
	if err != nil {
		log.Fatal(err)
	}
	n, err := io.Copy(io.Discard, gz)
	if err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("decompressed %d bytes in %v (%.0f MB/s)\n",
		n, elapsed, float64(n)/elapsed.Seconds()/1e6)
}
