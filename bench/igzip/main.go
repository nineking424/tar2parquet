package main

// ISA-L igzip 해제 단독 처리량 측정 (bench/gunzip의 klauspost 대조군).
// 파이프라인이 쓰는 것과 동일한 igzip.Reader 경로를 측정한다.

import (
	"bufio"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"tar2parquet/igzip"
)

func main() {
	sum := flag.Bool("sum", false, "해제 결과의 CRC32(IEEE)를 함께 출력 (정합성 대조용)")
	flag.Parse()

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var out io.Writer = io.Discard
	h := crc32.NewIEEE()
	if *sum {
		out = h
	}

	start := time.Now()
	z, err := igzip.NewReader(bufio.NewReaderSize(f, 4<<20))
	if err != nil {
		log.Fatal(err)
	}
	defer z.Close()
	n, err := io.Copy(out, z)
	if err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("decompressed %d bytes in %v (%.0f MB/s)\n",
		n, elapsed, float64(n)/elapsed.Seconds()/1e6)
	if *sum {
		fmt.Printf("crc32 %08x\n", h.Sum32())
	}
}
