# tar2parquet

`A.tar.gz`(내부에 동일 스키마 CSV 여러 개)를 단일 패스 스트리밍으로 읽어
하나의 `A.parquet`(zstd)으로 변환하는 CLI. 제약사항은 [REQUIREMENTS.md](REQUIREMENTS.md) 참조.

```bash
# 로컬 개발 빌드. 전제조건: ISA-L (gzip 해제 — docs/adr/0002 참조)
#   macOS: brew install isa-l
#   linux: 소스 빌드(정적 링크용 libisal.a — build/Dockerfile.rocky8 절차 참조)
go build .
./tar2parquet A.tar.gz   # → A.parquet
```

환경변수 `TAR2PARQUET_THREADS`로 병렬도 상한을 제한할 수 있다(기본: 코어 수).
`TAR2PARQUET_CPUPROFILE=<path>`를 주면 pprof CPU 프로파일을 기록한다(성능 분석용).

## 릴리스 빌드 (배포용 linux 바이너리)

배포 대상은 **Rocky Linux 8.6(glibc 2.28)**이다. 로컬/CI의 최신 배포판에서
그냥 빌드한 바이너리는 프로덕션에서 뜨지 않는다 — `GLIBC_2.34` 계열과
`GLIBCXX_3.4.30`을 요구하기 때문이다(근거와 결정: [ADR 0003](docs/adr/0003-glibc-target-rocky8.md)).
릴리스는 반드시 아래 경로로 만든다:

```bash
build/release.sh          # rockylinux:8 컨테이너 빌드 → 심볼 검증
                          # → rockylinux:8.6 실행 검증 → dist/ 패키징
```

Docker만 있으면 되고, 결과는 `dist/`에 떨어진다. 스크립트는 GLIBC 요구
버전이 2.28을 넘거나 libisal/libstdc++ 동적 의존이 남으면 실패한다.
prebuilt 배포 채널은 [GitHub Releases](https://github.com/nineking424/tar2parquet/releases)다.

## 아키텍처

```
A.tar.gz ─(prefetch 4MiB×2)─> igzip(ISA-L) ─> tar ─> 헤더 제거/개행 보정
      ─> 행 경계 블록 분할(~2MiB) ─> 유한 채널(depth 4)
                                        │
DuckDB COPY (SELECT * FROM tar_csv()) TO A.parquet (zstd)
      └─ Table UDF FillChunk: 멀티스레드가 블록 단위로 CSV 파싱 + 청크 적재
```

- **파일 = 모듈**: `main.go`(파이프라인 조립 + `feed` 동시성 규약) ·
  `producer.go`(prefetch·igzip 해제·tar 순회·행 경계 블록 분할) ·
  `consumer.go`(Table UDF 파싱·적재) · `chunkfill.go`(적재 fast path) ·
  `schema.go`(스키마 확정) · `igzip/`(해제 reader).
  각 파일 머리 주석이 그 모듈의 interface(계약·불변식)다.
- **Table UDF 공급**: DuckDB `read_csv` + pipe(`/dev/fd/N`) 조합은
  duckdb-go v2.10504에서 바인딩 시점 스키마가 placeholder(`column0`)로
  잡히고 0행을 반환한다(raw connection 실행으로도 재현, 조용한 데이터 소실).
  대신 `ParallelChunkTableSource`로 Go가 행 블록을 직접 공급하며,
  이 경로는 CSV 파싱과 Parquet 인코딩이 전 코어로 병렬화된다.
- **고속 적재 경로**(`chunkfill.go`): FillChunk이 셀 단위 `SetChunkValue`
  대신 DuckDB 벡터 메모리에 typed slice로 직접 쓴다. ≤12바이트 문자열은
  `duckdb_string_t` inline 표현으로 cgo 호출 없이 쓰고, 지수 없는 ≤15자리
  숫자는 Clinger fast path로 파싱한다(strconv와 비트 동일, 그 외 폴백).
  드라이버 구조체 레이아웃 검증 실패 시 기존 경로로 자동 폴백.
  효과: 표준 워크로드에서 user CPU 34% 감소(PERFORMANCE.md §10).
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

표준 샘플(50MiB CSV × 119개, 113M행/6.24GB) 기준 상세 리포트는
**[PERFORMANCE.md](PERFORMANCE.md)** 참조. 요약(2026-08-07, §10 벡터 직접
쓰기 + §11 igzip 이후): Xeon 4코어+NFS에서 warm 22.4s(§10 대비 −51%),
cold 28.1s(NFS read 하한). 병목은 consumer(파싱+인코딩)로, igzip 하한
(11s)까지는 ~6-7코어까지 코어를 늘릴수록 wall이 준다.

아래는 초기 검증(44M행 / CSV 2.43GB / tar.gz ~1GB) 수치.

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
