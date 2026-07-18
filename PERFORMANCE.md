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
