#!/bin/bash
# 릴리스 산출물의 런타임 요구를 검증한다(ADR 0003 수용 기준).
#
#   1) GLIBC 요구 최대 버전 <= 2.28          (Rocky Linux 8.6이 제공하는 상한)
#   2) GLIBCXX <= 3.4.25, CXXABI <= 1.3.11   (Rocky 8 기본 libstdc++ = GCC 8.5)
#   3) libisal 동적 의존 없음                (ADR 0002 — 정적 링크 유지)
#
# libstdc++.so.6 동적 의존 자체는 허용한다. duckdb-go-bindings가 cgo LDFLAGS에
# `-lstdc++`를 명시하고 있어 gcc의 -static-libstdc++로는 그 참조까지 없앨 수
# 없다(드라이버가 자동 추가하는 -lstdc++만 치환한다). 실질 기준은 "libstdc++가
# 없는가"가 아니라 "Rocky 8이 제공하는 심볼만 요구하는가"이므로 (2)로 판정한다.
# 실행 가능 여부의 최종 판정은 rockylinux:8.6 실행 검증이다(build/release.sh).
set -euo pipefail

BIN=${1:?usage: verify-glibc.sh <binary>}
MAX_GLIBC=${MAX_GLIBC:-2.28}
MAX_GLIBCXX=${MAX_GLIBCXX:-3.4.25}
MAX_CXXABI=${MAX_CXXABI:-1.3.11}

echo "== 검증 대상: $BIN =="
objdump -f "$BIN" | sed -n '2,3p'

fail=0

# 심볼 버전 집합이 상한 이하인지 검사한다.
#   $1 접두사(GLIBC/GLIBCXX/CXXABI), $2 상한
check_versions() {
    local prefix=$1 max=$2 vers over
    vers=$(objdump -T "$BIN" | sed -n "s/.*(${prefix}_\([0-9.]*\)).*/\1/p" | sort -u -V)
    if [ -z "$vers" ]; then
        echo "OK: ${prefix} 요구 없음"
        return
    fi
    echo "${prefix} 요구: $(echo "$vers" | tr '\n' ' ')"
    over=$(echo "$vers" | awk -v max="$max" '
        function cmp(a, b,   x, y, i) {
            split(a, x, "."); split(b, y, ".")
            for (i = 1; i <= 4; i++) { if ((x[i]+0) != (y[i]+0)) return (x[i]+0) - (y[i]+0) }
            return 0
        }
        cmp($0, max) > 0 { print }')
    if [ -n "$over" ]; then
        echo "  FAIL: 상한 ${max} 초과: $(echo "$over" | tr '\n' ' ')"
        for v in $over; do
            echo "    ${prefix}_$v: $(objdump -T "$BIN" | sed -n "s/.*(${prefix}_${v})[[:space:]]*//p" | sort | tr '\n' ' ')"
        done
        fail=1
    else
        echo "  OK: 최대 ${prefix}_$(echo "$vers" | tail -1) (상한 ${max})"
    fi
}

echo
echo "-- (1) glibc --"
check_versions GLIBC "$MAX_GLIBC"

echo
echo "-- (2) C++ 런타임 --"
check_versions GLIBCXX "$MAX_GLIBCXX"
check_versions CXXABI "$MAX_CXXABI"

echo
echo "-- (3) 동적 의존 --"
needed=$(objdump -p "$BIN" | awk '/NEEDED/ {print $2}')
echo "NEEDED: $(echo "$needed" | tr '\n' ' ')"
if echo "$needed" | grep -q '^libisal'; then
    echo "  FAIL: libisal 동적 의존이 남아 있다 (ADR 0002 정적 링크 위반)"
    fail=1
else
    echo "  OK: libisal 동적 의존 없음"
fi

if [ "$fail" -ne 0 ]; then
    echo
    echo "== 검증 실패 =="
    exit 1
fi
echo
echo "== 검증 통과 =="
