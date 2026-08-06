#!/bin/bash
# 릴리스 바이너리 빌드 — Dockerfile.rocky8 이미지 안에서 실행된다.
# 입력: /src(레포, 읽기 전용 마운트), 출력: /out
set -euo pipefail

source /opt/rh/gcc-toolset-12/enable

OUT=${OUT:-/out}
mkdir -p "$OUT"

echo "== 툴체인 =="
go version
gcc --version | head -1
ldd --version | head -1

# -static-libstdc++: 사전 컴파일된 DuckDB 정적 라이브러리가 요구하는
# GLIBCXX_3.4.29/3.4.30 심볼을 정적 libstdc++.a에서 해결한다. 이게 없으면
# Rocky 8의 libstdc++.so.6(3.4.25 상한)로는 그 심볼을 못 찾아 링크가 실패한다.
# 단, duckdb-go-bindings가 cgo LDFLAGS에 `-lstdc++`를 명시하고 있어
# libstdc++.so.6 동적 의존 자체는 남는다 — 남은 동적 요구는 GLIBCXX_3.4.22
# 이하라 Rocky 8이 제공하는 범위 안이다(실측). 상세는 ADR 0003.
# isa-l은 -l:libisal.a로 이미 정적 링크된다(igzip 패키지 cgo LDFLAGS, ADR 0002).
LDFLAGS="-s -w -linkmode external -extldflags '-static-libstdc++ -static-libgcc'"

echo "== build =="
cd /src
for target in "tar2parquet:." "gen:./bench/gen" "check:./bench/check"; do
    name=${target%%:*}
    pkg=${target#*:}
    echo "-- $name ($pkg)"
    go build -trimpath -ldflags "$LDFLAGS" -o "$OUT/$name" "$pkg"
done

echo "== 산출물 =="
ls -l "$OUT"

/usr/local/bin/verify-glibc.sh "$OUT/tar2parquet"
