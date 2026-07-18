package main

// 합성 A.tar.gz 생성기: REQUIREMENTS 스키마의 CSV 파일 -files개를
// 각각 약 -mb MiB 크기로 담는다.

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/klauspost/compress/gzip"
)

const csvHeader = "Col,Row,ChipX,ChipY,WaferX,WaferY,Height,Zone\n"

func main() {
	files := flag.Int("files", 119, "number of csv files")
	mb := flag.Int("mb", 50, "approx size of each csv in MiB")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("usage: gen [-files N] [-mb M] A.tar.gz")
	}

	f, err := os.Create(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gz, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
	tw := tar.NewWriter(gz)

	target := *mb << 20
	var buf bytes.Buffer
	var totalRows, totalBytes int64
	g := 0 // 전역 row 인덱스: 파일 간 내용이 달라지도록

	for i := 1; i <= *files; i++ {
		buf.Reset()
		buf.WriteString(csvHeader)
		for buf.Len() < target {
			r := g
			g++
			buf.WriteString(strconv.Itoa(r % 500))
			buf.WriteByte(',')
			buf.WriteString(strconv.Itoa(r % 700))
			buf.WriteByte(',')
			buf.WriteString(strconv.FormatFloat(float64(r%1000)*0.137, 'f', 3, 64))
			buf.WriteByte(',')
			buf.WriteString(strconv.FormatFloat(float64(r%900)*0.211, 'f', 3, 64))
			buf.WriteByte(',')
			buf.WriteString(strconv.FormatFloat(float64(r%777)*1.618, 'f', 4, 64))
			buf.WriteByte(',')
			buf.WriteString(strconv.FormatFloat(float64(r%888)*2.718, 'f', 4, 64))
			buf.WriteByte(',')
			buf.WriteString(strconv.FormatFloat(float64(r%333)*0.001, 'f', 5, 64))
			buf.WriteString(",zone")
			buf.WriteString(strconv.Itoa(r % 16))
			buf.WriteByte('\n')
			totalRows++
		}

		hdr := &tar.Header{
			Name:     fmt.Sprintf("A-%d.csv", i),
			Mode:     0o644,
			Size:     int64(buf.Len()),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			log.Fatal(err)
		}
		if _, err := io.Copy(tw, bytes.NewReader(buf.Bytes())); err != nil {
			log.Fatal(err)
		}
		totalBytes += int64(buf.Len())
	}

	if err := tw.Close(); err != nil {
		log.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("files=%d rows=%d csv_bytes=%d\n", *files, totalRows, totalBytes)
}
