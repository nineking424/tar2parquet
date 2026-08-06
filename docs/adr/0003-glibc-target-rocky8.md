# 0003. 릴리스 빌드 환경을 Rocky Linux 8(glibc 2.28)로 고정한다

- 상태: 승인 (2026-08-07)

## 맥락

프로덕션은 Rocky Linux 8.6(glibc 2.28)이다. 그러나 v0.1.0 prebuilt는
Debian bookworm(golang:1.25 이미지)에서 빌드돼 실행이 거부된다. 추정이
아니라 실측이다 — `objdump -T tar2parquet-linux-amd64`:

- glibc 2.28 **초과 요구 심볼 37개**, 최대 `GLIBC_2.38`
  - `GLIBC_2.29`(4): `exp` `log` `log2` `pow`
  - `GLIBC_2.32`(2): `pthread_getattr_np` `pthread_sigmask`
  - `GLIBC_2.34`(29): `__libc_start_main` `pthread_create` `pthread_join`
    `dlopen` `dlsym` `sem_wait` 등 — glibc 2.34에서 libpthread/libdl이
    libc로 통합되며 심볼 버전이 올라간 것들
  - `GLIBC_2.38`(2): `fmod` `fmodf`
- C++ 런타임도 별개로 미충족: `GLIBCXX_3.4.26` `GLIBCXX_3.4.29`
  `GLIBCXX_3.4.30` `CXXABI_1.3.13` 요구. Rocky 8의 기본
  libstdc++(GCC 8.5)는 `GLIBCXX_3.4.25`까지만 제공한다.

`rockylinux:8.6` 컨테이너에서 실행하면 동적 로더가 위 8종 버전을 모두
`not found`로 보고하고 종료한다(exit 1). 즉 현행 prebuilt는 프로덕션에서
**아예 뜨지 않는다**.

요구 심볼의 출처는 두 갈래다. glibc 쪽은 빌드 호스트의 glibc가 결정하므로
빌드 환경을 바꾸는 것 외에 방법이 없다(링커는 호스트 glibc가 제공하는
최신 버전 노드에 바인딩한다). C++ 쪽은 `duckdb-go-bindings`가 배포하는
**사전 컴파일 정적 라이브러리**가 최신 GCC로 빌드돼 있어서 생긴다 —
소스가 우리 손에 없으므로 컴파일러 선택으로 낮출 수 없다.

## 결정

릴리스 빌드 환경을 `rockylinux:8` 컨테이너로 고정하고, 재현 가능한
스크립트(`build/Dockerfile.rocky8`, `build/release.sh`)로 커밋한다.

- **베이스**: `rockylinux:8`(현행 8.10). 8.x 계열은 glibc 2.28로 동일하고
  심볼 버전 상한도 `GLIBC_2.28`이다. 8.6 이미지를 빌드 베이스로 쓰지 않는
  이유는 EOL이라 dnf 리포지토리가 vault로 이동해 패키지 설치가 불가하기
  때문이다. **실행 검증은 프로덕션과 같은 `rockylinux:8.6`에서** 한다.
- **Go**: 공식 tarball을 버전+sha256으로 고정(현재 1.24.13). Rocky 8의
  go-toolset은 1.24 미만이라 쓸 수 없다. 즉 "구형 배포판 = 구형 툴체인"이
  아니다 — 낡게 고정되는 것은 glibc뿐이다.
- **C++ 런타임**: `gcc-toolset-12`로 빌드하고 `-static-libstdc++
  -static-libgcc`를 준다. gcc-toolset-12의 정적 libstdc++가 DuckDB의
  `GLIBCXX_3.4.29/3.4.30` 참조를 해결한다 — 이게 없으면 Rocky 8의
  libstdc++.so.6(3.4.25 상한)로는 심볼을 못 찾아 **링크 자체가 실패**한다.
  단 `libstdc++.so.6` 동적 의존은 남는다: 바인딩이 cgo LDFLAGS에
  `-lstdc++`를 명시하고 있고, gcc의 `-static-libstdc++`는 드라이버가 자동
  추가하는 `-lstdc++`만 치환하기 때문이다. 남은 동적 요구는
  `GLIBCXX_3.4.22`·`CXXABI_1.3.8` 이하로 Rocky 8 제공 범위 안이다(실측).
- **ISA-L**: v2.31.0 소스 빌드 + `libisal.a` 정적 링크를 유지한다(ADR 0002).
  nasm은 Rocky 8의 powertools(CRB) 리포지토리에서 받는다(2.15.03).
- **검증 없이 릴리스하지 않는다**: `build/verify-glibc.sh`가 (1) GLIBC ≤ 2.28,
  (2) GLIBCXX ≤ 3.4.25 · CXXABI ≤ 1.3.11(Rocky 8 기본 libstdc++ 상한),
  (3) libisal 동적 의존 부재를 확인하고, 실패 시 빌드가 non-zero로 죽는다.
  판정 기준은 "정적 링크가 됐는가"가 아니라 "Rocky 8이 제공하는 심볼만
  요구하는가"다 — 최종 판정은 `rockylinux:8.6` 실행 검증이다.
- **배포 채널**: GitHub Releases (`gh release create`). 이 결정 이전에는
  레포에 기록이 없었다 — v0.1.0이 사실상 그 채널이었음을 확인하고 명문화한다.

## 대안과 기각 사유

- **bookworm 유지 + glibc 정적 링크**: cgo를 쓰는 Go 바이너리에서 glibc
  전체 정적 링크는 NSS(`getaddrinfo` 등)가 런타임에 같은 버전 glibc를
  요구해 깨지는 알려진 함정이다. DuckDB·ISA-L 두 cgo 의존이 있는 상태로는
  더 위험하다.
- **DuckDB 소스 빌드**: 사전 컴파일 정적 라이브러리의 C++ 심볼 요구를
  낮추는 확실한 방법이지만, 빌드 시간이 수십 분대로 늘고 업스트림
  바이너리와의 동등성 검증 부담이 생긴다. `-static-libstdc++`로 같은
  결과를 훨씬 싸게 얻으므로 채택하지 않는다. 미래에 바인딩이 더 새로운
  GLIBCXX를 요구해도 이 결정은 유효하다 — 정적 링크가 그 요구를 흡수한다.
- **프로덕션 OS 업그레이드**: 우리 통제 밖이다.

## 검증 결과 (2026-08-07, `rockylinux:8` 빌드 → `rockylinux:8.6` 실행)

- 툴체인: Go 1.24.13 / GCC 12.2.1 / glibc 2.28
- 심볼: GLIBC 최대 요구 **2.25**(≤2.28), GLIBCXX 최대 **3.4.22**(≤3.4.25),
  CXXABI 최대 **1.3.8**(≤1.3.11). NEEDED에 libisal 없음.
  대조군인 v0.1.0은 GLIBC 2.38 / GLIBCXX 3.4.30을 요구했다.
- 실행(`rockylinux:8.6` 순정 이미지, 빌드 툴체인 없음): 축소 샘플 변환 성공,
  `check` 정합성 통과(`rows=455342 sum(Col)=113580811 sum(Row)=159080811
  Height=[0,0.332] zones=16`), `TAR2PARQUET_THREADS=1` 재실행도 동일.
- 오류 경로도 확인했다(혼합 링크에서 C++ 예외 전파가 깨지지 않는지가 관건):
  비-gzip 입력 → `igzip: invalid gzip header`, 잘린 아카이브 → DuckDB
  `Invalid Input Error: unexpected EOF`, 없는 파일 조회 → `IO Error`.
  세 경우 모두 exit 1과 tmp 정리까지 정상.

## 결과

- 빌드 호스트가 arm64(개발용 Mac)면 linux/amd64는 에뮬레이션이라 릴리스
  빌드가 느리다. 릴리스는 자주 하지 않으므로 수용한다.
- k8s의 perf/bench Job은 **측정용**이라 `golang:1.24`(bookworm)를 유지한다.
  빌드와 실행이 같은 컨테이너에서 일어나 호환 문제가 없다. 릴리스 경로와
  혼동되지 않게 매니페스트 주석으로 구분했다.
- 로컬 개발(`go build .`)은 종전과 같다. 이 ADR이 규정하는 것은 배포용
  산출물을 만드는 경로뿐이다.
- glibc 타깃을 낮추는 결정이라 되돌리기 어렵다. 프로덕션이 Rocky 9 이상으로
  올라가면 `Dockerfile.rocky8`의 베이스와 `MAX_GLIBC`만 바꾸면 된다.
