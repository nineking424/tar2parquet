#!/usr/bin/env bash
# Rocky Linux 8.6(glibc 2.28) 호환 릴리스 빌드 — 한 줄 재현 진입점 (ADR 0003).
#
#   build/release.sh              # 빌드 + 심볼 검증 + rockylinux:8.6 실행 검증 + 패키징
#   build/release.sh --no-smoke   # 실행 검증 생략(빌드/심볼 검증만)
#
# 산출물: dist/linux-amd64/{tar2parquet,gen,check}
#         dist/tar2parquet-linux-amd64.tar.gz, dist/SHA256SUMS.txt
#
# 호스트가 arm64(Apple Silicon)면 linux/amd64는 에뮬레이션이라 느리다.
# 정상이며, 첫 실행은 이미지 빌드 포함 수십 분이 걸릴 수 있다.
set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DOCKER=${DOCKER:-docker}
IMAGE=${IMAGE:-tar2parquet-release:rocky8}
PLATFORM=linux/amd64
RUNTIME_IMAGE=${RUNTIME_IMAGE:-rockylinux:8.6}   # 프로덕션과 동일한 마이너 버전
OUT=$REPO/dist/linux-amd64

smoke=1
[ "${1:-}" = "--no-smoke" ] && smoke=0

mkdir -p "$OUT"

echo "############ 1/4 빌드 환경 이미지 ############"
$DOCKER build --platform "$PLATFORM" -t "$IMAGE" -f "$REPO/build/Dockerfile.rocky8" "$REPO/build"

echo "############ 2/4 릴리스 빌드 + 심볼 검증 ############"
# 레포는 읽기 전용으로 마운트한다(빌드가 작업 트리를 오염시키지 않게).
# 모듈/빌드 캐시는 named volume에 두어 재실행을 빠르게 한다.
$DOCKER run --rm --platform "$PLATFORM" \
    -v "$REPO":/src:ro \
    -v "$OUT":/out \
    -v tar2parquet-gomod:/go/pkg/mod \
    -v tar2parquet-gocache:/root/.cache/go-build \
    "$IMAGE"

if [ "$smoke" -eq 1 ]; then
    echo "############ 3/4 실행 검증 ($RUNTIME_IMAGE) ############"
    # 축소 샘플(3 x 8MiB)로 변환 + check 정합성. 빌드 툴체인이 전혀 없는
    # 순정 Rocky 8.6 이미지에서 도는지가 요점이다.
    $DOCKER run --rm --platform "$PLATFORM" -v "$OUT":/out:ro "$RUNTIME_IMAGE" bash -eo pipefail -c '
        cat /etc/rocky-release; ldd --version | head -1
        echo "-- ldd (libisal이 없어야 한다 — ADR 0002 정적 링크) --"
        ldd /out/tar2parquet
        cd "$(mktemp -d)"
        echo "-- 축소 샘플 생성 --"
        /out/gen -files 3 -mb 8 A.tar.gz && ls -l A.tar.gz
        echo "-- 변환 --"
        time /out/tar2parquet A.tar.gz
        ls -l A.parquet
        echo "-- check 정합성 --"
        /out/check A.parquet
    '
else
    echo "############ 3/4 실행 검증 생략(--no-smoke) ############"
fi

echo "############ 4/4 패키징 ############"
tar -C "$OUT" -czf "$REPO/dist/tar2parquet-linux-amd64.tar.gz" tar2parquet
(cd "$REPO/dist" && shasum -a 256 tar2parquet-linux-amd64.tar.gz > SHA256SUMS.txt)
ls -l "$REPO/dist"
cat "$REPO/dist/SHA256SUMS.txt"
echo "== 완료 =="
