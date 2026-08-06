# tar2parquet 성능 테스트 리포트

- **일자**: 2026-07-18
- **대상 커밋**: `525644a` (main)
- **샘플 규격**: 50MiB CSV × **119개**를 하나의 tar.gz으로 묶은 표준 샘플
- **테스트 환경**: 로컬 워크스테이션(Apple M4)과 운영 유사 환경(k8s + NFS) 2종

## 1. 샘플 데이터

`bench/gen`(결정적 생성기, REQUIREMENTS 스키마 8컬럼)으로 생성.

| 항목 | 값 |
|---|---|
| CSV 파일 수 | 119개 (`A-1.csv` … `A-119.csv`, 각 ~50MiB) |
| 총 행 수 | 112,885,096 |
| CSV 총 크기 | 6,239,030,469 B (6.24GB) |
| tar.gz 크기 | 2,685,583,669 B (2.69GB, 압축률 43%) |
| 스키마 | Col,Row(BIGINT) / ChipX,ChipY,WaferX,WaferY,Height(DOUBLE) / Zone(VARCHAR) |
| 출력 parquet | 61MB (zstd; 합성 데이터 특성상 압축률이 실데이터보다 높을 수 있음) |

## 2. 테스트 환경

| | 로컬 | k8s NFS |
|---|---|---|
| CPU | Apple M4, 10코어 | Intel Xeon E5-2696 v3 @2.3GHz, **4코어 할당** |
| 메모리 | 16GB | request 3Gi / limit 6Gi |
| 저장소 | NVMe (로컬) | NFS (nfs-subdir-external-provisioner, NAS 192.168.1.4) |
| OS | macOS 15.2 | Talos Linux 1.13 / golang:1.24 컨테이너 |
| 측정 도구 | `/usr/bin/time -l` | GNU time (`-v`), `dd` |

k8s는 **3단계 Job을 서로 다른 워커 노드에 고정**해 page cache 오염 없이
cold 수치를 측정했다: 생성(wk-03) → read baseline(wk-02) → 변환 벤치(wk-01).

## 3. 저장소 baseline (k8s NFS)

| 항목 | 결과 |
|---|---|
| NFS 순차 쓰기 (dd 1GiB, fsync) | **114 MB/s** |
| NFS 순차 읽기, cold (2.69GB) | **116 MB/s** (23.2s) |
| NFS 순차 읽기, warm (page cache) | 383 MB/s |

## 4. 단계별 분해: gzip 해제 단독 (CPU 이론 상한)

전체 파이프라인의 유일한 직렬 구간인 단일 스트림 gzip 해제만 분리 측정
(`bench/gunzip`, 6.24GB 출력 기준).

| 환경 | 시간 | 처리량 |
|---|---|---|
| Apple M4 | 27.50s / 28.32s | 227 / 220 MB/s |
| Xeon (k8s) | 43.58s / 43.81s | 143 / 142 MB/s |

단, 이 단독 측정치는 *읽기 syscall + 복사 + 해제*가 한 스레드에 직렬화된
수치다. 실제 파이프라인은 prefetch goroutine이 읽기를 분리하므로 producer
스레드의 순수 해제 시간은 이보다 짧다(로컬 결과에서 확인됨, §5).

## 5. 전체 변환 성능

### 로컬 (Apple M4, 기본 = 10스레드)

| 회차 | wall | user | sys | peak RSS |
|---|---|---|---|---|
| 1 | 24.61s | 63.27s | 2.70s | 373MB |
| 2 | **24.05s** | 63.42s | 2.48s | 363MB |
| 3 | 24.09s | 63.22s | 2.61s | 395MB |

- **처리량: CSV 기준 259 MB/s, 4.69M rows/s** (tar.gz 기준 112 MB/s)
- 변환 wall(24.1s)이 gunzip 단독(27.5s)보다 **빠르다**: prefetch가 디스크
  읽기+복사를 별도 코어로 분리해 producer가 순수 해제만 수행하기 때문.
  즉 파이프라인 오버헤드는 사실상 0이며, 실질 병목은 순수 gzip 해제(~24s).

### k8s NFS (Xeon 4코어, 기본 = 4스레드)

| 회차 | 조건 | wall | user | sys | CPU | peak RSS |
|---|---|---|---|---|---|---|
| 1 | **cold** (빈 page cache) | 57.81s | 177.7s | 8.0s | 321% | 232MB |
| 2 | warm | **53.68s** | 175.0s | 5.0s | 335% | 233MB |
| 3 | warm | 53.95s | 176.4s | 5.2s | 336% | 228MB |

- **처리량: CSV 기준 116 MB/s, 2.10M rows/s** (warm 기준)
- **cold와 warm의 차이가 4.1s(+7.6%)에 불과**: NFS에서 2.69GB를 읽는 데
  단독으로 23.2s가 걸리지만(§3), prefetch가 읽기를 해제와 겹쳐 wall time에
  거의 가산되지 않는다. **"디스크 I/O가 최대 병목"인 환경에서 의도한
  설계 효과가 실측으로 확인됨.**
- 정합성: 두 환경 모두 행 수 112,885,096, 집계값(sum/min/max/distinct),
  8컬럼 스키마 전부 일치.

## 6. 스레드 스케일링

`TAR2PARQUET_THREADS`로 병렬도 상한을 제어하며 warm 상태에서 측정.

| threads | 로컬 M4 wall | k8s Xeon wall | k8s CPU |
|---|---|---|---|
| 1 | 40.11s | 122.99s | 139% |
| 2 | 23.66s | 64.52s | 271% |
| 4 | 23.69s | 54.92s | 337% |
| 8 | 23.77s | — | — |
| 10 | 23.79s | — | — |

- **로컬(M4)**: 2스레드에서 이미 포화. 소비자(파싱+Parquet 인코딩) 총 CPU가
  ~39s라 빠른 코어 2개면 producer(해제 ~24s)를 따라잡는다. 이후는 producer
  병목이라 코어를 늘려도 wall이 줄지 않는다.
- **k8s(Xeon 4코어)**: 4스레드까지 계속 개선(123s → 64.5s → 54.9s).
  총 CPU 수요 ~180s ÷ 4코어 = 45s가 자원 하한이며 wall 53.7s는 그 대비
  84% 효율. 이 환경은 producer 병목이 아니라 **코어 수 병목**이므로
  코어를 더 할당하면 gzip 한계(~44s)까지 단축 여지가 있다.
- peak RSS는 1스레드 155MB ~ 10스레드 422MB 범위로, 데이터 크기(6.24GB)와
  무관한 고정 수준(§10 메모리 제약 충족).

## 7. 병목 분석 요약

```
          producer(직렬)                    consumers(병렬)
  NFS read ─┐
            ├─ gzip 해제 ─ tar ─ 블록 분할 ─▶ CSV 파싱 ─ Parquet 인코딩(zstd)
  (prefetch로 겹침)                          (threads 개 스레드)
```

| 환경 | 지배 요인 | 근거 |
|---|---|---|
| M4 (코어 여유) | 단일 스트림 gzip 해제 | wall ≈ 순수 해제 시간, threads≥2에서 포화 |
| Xeon 4코어 | CPU 총량(코어 수) | wall ≈ 총 CPU/4, 4T까지 선형 개선 |
| NFS I/O | **병목 아님** | cold vs warm +7.6%, 읽기 23.2s가 wall에 미가산 |

추가 개선은 현 구조 내에서는 소진 상태이며, 남은 지렛대는 입력 포맷 변경
(zstd 또는 블록 단위 gzip으로 병렬 해제 가능화)과 더 많은 코어 할당뿐이다.

## 8. 재현 방법

```bash
# 로컬
go build -o t2p . && go build -o gen ./bench/gen \
  && go build -o check ./bench/check && go build -o gunzip ./bench/gunzip
./gen A.tar.gz            # 119 x 50MiB (기본값)
./gunzip A.tar.gz         # 해제 단독 상한
time ./t2p A.tar.gz       # 변환
./check A.parquet         # 정합성 검증
TAR2PARQUET_THREADS=2 ./t2p A.tar.gz   # 스케일링

# k8s (NFS 기본 storage class)
kubectl apply -f k8s/perf/01-gen.yaml       # 완료 대기 후
kubectl apply -f k8s/perf/02-coldread.yaml  # 완료 대기 후
kubectl apply -f k8s/perf/03-bench.yaml
kubectl logs job/tar2parquet-perf-bench
```

## 9. 한계 및 주의

- 합성 데이터는 압축률(43%)과 값 분포가 실데이터와 다를 수 있다. gzip
  해제 속도는 압축률에 민감하므로 실데이터로 상한(§4)을 재측정할 것을 권장.
- quoted field 내 개행은 미지원(행 경계 블록 분할 전제, README 참조).
- k8s warm 수치는 page cache(limit 6Gi > 아카이브 2.69GB) 영향을 포함하나,
  cold 실측(run 1)과의 차이가 작아 결론에 영향 없음.

## 10. 후속 최적화: 벡터 직접 쓰기 + 고속 파서 (2026-08-07)

§7의 "현 구조 내 소진" 결론 이후, 프로파일(pprof, 계측 플래그
`TAR2PARQUET_CPUPROFILE`)로 consumer 경로를 재분석해 두 가지를 적용했다.
목표 지표는 **user CPU**(1코어~수십 코어 스펙트럼에서 저코어 wall에 1:1
반영, 다코어에서 무손해 — `docs/adr/0001` 참조).

1. **벡터 직접 쓰기**(`chunkfill.go`): 셀 단위 `SetChunkValue`가
   VARCHAR마다 cgo 호출(행 113M × 1컬럼), 제네릭 디스패치, projection 맵
   조회를 수행 — 합계 총 CPU의 ~15%. DuckDB 벡터의 data 배열에 typed
   slice로 직접 쓰고, ≤12바이트 문자열은 `duckdb_string_t` inline으로
   cgo 없이, NULL은 validity 비트 직접 조작으로 대체했다. 드라이버
   레이아웃/타입 검증 실패 시 기존 경로 자동 폴백.
2. **고속 숫자 파서**: `strconv.ParseFloat` 경로가 총 CPU의 ~8%.
   지수 없는 ≤15자리 십진수는 Clinger fast path(정수 누적 후 10^frac
   나눗셈 1회 = 단일 IEEE 반올림)로 처리한다. strconv와 **비트 단위 동일**
   함을 무작위 10,000케이스 대조 테스트로 보장하고, 그 외 형식은 strconv
   폴백.

### 측정 (20 × 50MiB 축소 샘플*, Apple M4, 3회 중앙값)

| 지표 | 이전 | 이후 | 변화 |
|---|---|---|---|
| user CPU (10T) | 11.23s | 7.36s | **−34%** |
| user CPU (4T) | 11.17s | 7.37s | **−34%** |
| user CPU (1T) | — | 7.25s | — |
| wall (10T) | 4.52s | 4.23s | −6% (producer 병목 유지) |
| peak RSS | 363~422MB | 219~437MB | 예산(1GB) 내 |

\* 로컬 디스크 여유 부족으로 축소 샘플 사용. user CPU는 데이터량에
선형이므로 비율 판정에 유효. 정합성(`bench/check`): 행 수 18,972,296,
집계값·스키마 변경 전과 완전 일치.

- 남은 CPU 분포(최적화 후): gzip 해제 ~35%, DuckDB 인코딩(zstd) ~30%,
  Go 스케줄러 wakeup(macOS `pthread_cond_signal`) ~25%, 파싱/적재 ~6%.
  앞 둘은 포맷/압축률 제약으로 봉인, wakeup은 macOS 특성으로 Linux
  (운영 환경)에서는 futex라 훨씬 저렴 — 블록 크기 4배 실험으로 채널
  send 빈도와 무관함을 확인했다(개발 머신 특화 최적화는 하지 않음).

### k8s 검증 (표준 샘플 119 × 50MiB, Xeon 4코어 + NFS, 커밋 `c2c05af`)

| 지표 | 이전(§5) | 이후 | 변화 |
|---|---|---|---|
| user CPU | 175.0~177.7s | 102.7~104.3s | **−41%** |
| wall cold | 57.81s | 49.69s | −14% |
| wall warm | 53.68s | **46.78s** | −13% |
| CPU 사용률 | 321~336% | 219~228% | 코어 수요 3.4 → 2.3 |
| peak RSS | 228~233MB | 225~228MB | 동일 수준 |

| threads | 이전 wall | 이후 wall | 변화 |
|---|---|---|---|
| 1 | 122.99s | 54.48s | **−56%** |
| 2 | 64.52s | 47.01s | −27% |
| 4 | 54.92s | 47.33s | −14% |

- 정합성: 행 수 112,885,096과 집계값·스키마 전부 이전과 일치.
- **병목 전환 확인**: 총 CPU가 ~180s → ~106s로 줄면서 4코어 환경도
  코어 수 병목에서 **producer(gzip) 병목으로 전환**됐다. 2스레드에서
  이미 wall이 포화(47s ≈ gzip 한계 44s + 파이프라인 슬랙)하므로,
  이 워크로드는 이제 **2코어 할당으로 충분**하다 — 같은 클러스터
  자원으로 동시 변환 처리량을 ~2배 늘릴 수 있다는 뜻.
- 저코어 효과는 예측대로 극적: 1스레드 wall −56%.

## 11. gzip 해제를 igzip(ISA-L)으로 교체 (2026-08-07)

§10이 consumer를 최적화한 뒤 남은 최대 소비처는 producer의 gzip 해제
(~43s user CPU, 총량의 ~40%)였다. 해제 라이브러리를 klauspost에서
ISA-L igzip(cgo)으로 교체했다 — 설계 결정과 빌드 전제조건(isa-l,
linux 정적 링크)은 `docs/adr/0002` 참조.

### 해제 단독 벤치 (go/no-go 게이트, `bench/gunzip` vs `bench/igzip`)

| 환경 | klauspost | igzip | 배율 |
|---|---|---|---|
| Xeon E5-2696v3 (표준 샘플, warm, user CPU) | 42.5~44.0s (140~145 MB/s) | 10.1~10.6s (559~591 MB/s) | **4.17x** |
| Apple M4 (축소 샘플 A20) | 255~257 MB/s | 492~498 MB/s | 1.94x |

해제 결과는 양 환경 모두 CRC32 일치(igzip은 gzip 트레일러 검증까지
켠 수치). 사전 합의한 게이트(Xeon 1.3x 미만이면 중단)를 크게 상회.

### 로컬 검산 (축소 샘플 A20, Apple M4)

| 지표 | 이전(§10) | igzip | 변화 |
|---|---|---|---|
| user CPU (10T) | 7.36s | 5.47~5.54s | **−26%** |
| wall (10T) | 4.23s | **2.17s** | −49%, igzip 단독 하한(2.1s) 도달 |
| wall (1T) | — | 3.59s | |
| peak RSS | ≤437MB | 356~405MB | 예산 내 |

단독 벤치 배율에서 예상한 절감(~2.0s)과 실측(1.9s)이 부합 — 통합
오버헤드(reader 복사 포함)는 수 %.

### k8s 검증 (표준 샘플 119 × 50MiB, Xeon 4코어 + NFS, 커밋 `9c6c769`)

| 지표 | 이전(§10) | igzip | 변화 |
|---|---|---|---|
| wall warm | 46.78s | **22.35~25.49s** | **−51%** |
| wall cold | 49.69s | 28.12s | −43% (NFS read 하한 ~29s 도달) |
| user CPU | 102.7~104.3s | 77.4~88.3s | −22% |
| CPU 사용률 | 219~228% | 304~365% | 코어 수요 2.3 → 3.6 |
| peak RSS | 225~228MB | 161~259MB | 예산 내 |

| threads | 이전 wall | igzip wall | igzip user CPU |
|---|---|---|---|
| 1 | 54.48s | 55.27s | 68.2s |
| 2 | 47.01s | 28.85s | 69.2s |
| 4 | 47.33s | 22.35s | 77.4s |

- 정합성: 행 수 112,885,096, 집계값·스키마 §10과 완전 일치.
- user CPU 절감(−22%)이 단독 벤치 예상(−32%)보다 작은 이유: 병렬도가
  높아지며(2.3→3.6코어) DuckDB 동기화 오버헤드가 스레드 수에 비례해
  증가(1T 68.2s → 4T 77.4s). 1T user CPU 기준으로는 예상과 부합.

### 병목 재판정

- **§10의 "2코어면 포화"는 폐기.** producer 하한이 44s→11s로 내려가
  4코어에서도 wall이 22s(361% CPU)로 **consumer(파싱+Parquet 인코딩)
  병목**이다. consumer 총량 ~70s 기준, igzip 하한(11s)에 닿으려면
  **~6-7코어**가 필요하다 — 그 지점까지는 코어를 늘릴수록 wall이 준다.
- 권장: 다코어 환경에서는 4코어+ 할당(wall ~22s), 처리량(동시 변환 수)
  우선이면 2코어 할당(wall ~29s)도 §10 대비 −39%.
- 1코어 wall은 55s로 §10과 동일 — 1T는 §10 시점에 이미 consumer-bound라
  igzip의 이득이 wall에 나타나지 않는다(user CPU로는 이득).
- cold run은 이제 NFS read(93MB/s, 압축 2.69GB ≈ 29s)가 하한이다.
  해제가 NFS보다 빨라져 prefetch로도 다 숨길 수 없다.

### 비 x86 환경 기대치

klauspost 폴백은 폐기됐다(ADR 0002) — isa-l이 빌드 전제조건이며,
ISA-L은 aarch64 최적화를 포함한다(Apple M4 실측 1.94x). x86/arm 외
아키텍처는 지원 대상이 아니다.
