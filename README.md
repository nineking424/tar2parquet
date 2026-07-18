# tar2parquet

`A.tar.gz`(내부에 동일 스키마 CSV 여러 개)를 단일 패스 스트리밍으로 읽어
하나의 `A.parquet`(zstd)으로 변환하는 CLI. 제약사항은 [REQUIREMENTS.md](REQUIREMENTS.md) 참조.

```bash
go build .
./tar2parquet A.tar.gz   # → A.parquet
```

## 아키텍처

```
A.tar.gz ─(prefetch 4MiB×2)─> gzip(klauspost) ─> tar ─> 헤더 제거/개행 보정
      ─> 행 경계 블록 분할(~2MiB) ─> 유한 채널(depth 4)
                                        │
DuckDB COPY (SELECT * FROM tar_csv()) TO A.parquet (zstd)
      └─ Table UDF FillChunk: 멀티스레드가 블록 단위로 CSV 파싱 + 청크 적재
```

- **Table UDF 공급**: DuckDB `read_csv` + pipe(`/dev/fd/N`) 조합은
  duckdb-go v2.10504에서 바인딩 시점 스키마가 placeholder(`column0`)로
  잡히고 0행을 반환한다(raw connection 실행으로도 재현, 조용한 데이터 소실).
  대신 `ParallelChunkTableSource`로 Go가 행 블록을 직접 공급하며,
  이 경로는 CSV 파싱과 Parquet 인코딩이 전 코어로 병렬화된다.
- **스키마**: 첫 CSV 헤더에서 컬럼명을 읽고, 알려진 컬럼
  (Col,Row→BIGINT / ChipX,ChipY,WaferX,WaferY,Height→DOUBLE / Zone→VARCHAR)은
  고정, 미지 컬럼은 첫 블록 샘플에서 BIGINT→DOUBLE→VARCHAR 순으로 추론.
- **스트리밍/메모리**: 아카이브 1회 읽기, 중간 파일 없음, 고정 크기
  버퍼만 사용(prefetch 8MiB + 블록 채널 ~8MiB + in-flight 블록).
- **오류 처리**: 출력은 `A.parquet.tmp`에 쓰고 성공 시 rename.
  스트림/파싱/쿼리 오류 시 tmp 제거 후 실패 종료.
- **함정 회피**: 드라이버 `ParallelTableSourceInfo.MaxThreads`에 0을 주면
  DuckDB에 max_threads=0이 그대로 전달되어 단일 스레드 스캔이 된다.
  `runtime.NumCPU()`를 명시해야 병렬화된다.

## 전제

- 모든 CSV는 동일 스키마·동일 헤더 (REQUIREMENTS §12).
- quoted field 안의 개행은 지원하지 않음(행 경계 블록 분할 전제).
  콤마·`""` escape는 지원.

## 성능

합성 데이터 44M행 / CSV 2.43GB / tar.gz ~1GB 기준.

로컬 (Apple Silicon, NVMe):

| 항목 | 시간 | 처리량 |
|---|---|---|
| gzip 해제 단독 (이론 상한) | 9.47s | 257 MB/s |
| 전체 변환 | 9.79s | 248 MB/s |

k8s NFS (Xeon E5-2696v3 4코어, nfs-client 기본 SC, NFS 쓰기 baseline 115MB/s):

| 항목 | 시간 | 비고 |
|---|---|---|
| 변환 1회차 | 22.6s | CPU 305% (4코어 중), CSV 기준 107 MB/s |
| 변환 2회차 | 21.3s | 반복 측정 일관 |

두 환경 모두 병목은 단일 스트림 gzip 해제이며 파이프라인 오버헤드는 수 %.
NFS read(~46MB/s 소요 대역)는 prefetch reader가 해제와 겹쳐 wall time에
추가되지 않음을 확인했다. user/real ≈ 3.05로 Table UDF 병렬 공급이
Linux/amd64에서도 동작함을 검증.

## 벤치마크 도구

```bash
go build -o gen ./bench/gen && ./gen A.tar.gz     # 합성 아카이브 생성
go build -o check ./bench/check && ./check A.parquet  # 행 수/집계/스키마 검증
```

## k8s(NFS) 검증

기본 storage class가 NFS인 클러스터에서:

```bash
kubectl apply -f k8s/bench-job.yaml
kubectl logs -f job/tar2parquet-bench
```

NFS 쓰기 baseline(dd) → 합성 아카이브 생성 → 변환 2회 → 결과 검증을 수행한다.
