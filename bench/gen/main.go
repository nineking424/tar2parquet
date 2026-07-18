package main

// 합성 A.tar.gz 생성기: REQUIREMENTS 스키마로 fileCount개의 CSV를 담는다.

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/klauspost/compress/gzip"
)

const (
	fileCount   = 8
	rowsPerFile = 5_500_000 // 전체 CSV 약 2.2GB
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: gen A.tar.gz")
	}

	f, err := os.Create(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gz, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
	tw := tar.NewWriter(gz)

	var buf bytes.Buffer
	totalRows := int64(0)
	totalBytes := int64(0)

	for i := 1; i <= fileCount; i++ {
		buf.Reset()
		buf.WriteString("Col,Row,ChipX,ChipY,WaferX,WaferY,Height,Zone\n")
		for r := 0; r < rowsPerFile; r++ {
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

	fmt.Printf("rows=%d csv_bytes=%d\n", totalRows, totalBytes)
}
