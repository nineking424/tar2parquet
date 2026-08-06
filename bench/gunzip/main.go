package main

// gzip 해제 단독 처리량 측정 (변환 파이프라인의 이론적 상한).

import (
	"bufio"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/klauspost/compress/gzip"
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
	gz, err := gzip.NewReader(bufio.NewReaderSize(f, 4<<20))
	if err != nil {
		log.Fatal(err)
	}
	n, err := io.Copy(out, gz)
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
