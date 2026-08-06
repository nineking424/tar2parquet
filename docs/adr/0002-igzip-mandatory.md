# 0002. gzip 해제를 ISA-L igzip으로 고정하고 isa-l을 빌드 전제조건으로 삼는다

- 상태: 승인 (2026-08-07)

## 맥락

ADR 0001이 수용한 직렬 gzip 하한은 해제 라이브러리 속도에 정비례한다.
해제 단독 실측에서 ISA-L igzip이 klauspost 대비 Xeon 4.17x(user CPU
43.4s→10.4s), M4 1.94x로 확인됐고, 표준 샘플 총 user CPU 103s 중 gzip이
~43s를 차지해 유일하게 남은 큰 지렛대였다.

## 결정

- 파이프라인의 gzip 해제는 igzip(cgo, `igzip` 패키지) **단일 경로**로
  한다. klauspost 폴백/선택 설계는 폐기한다 — 이득이 커서 igzip을 쓸 수
  없는 환경을 지원 대상에서 제외하는 쪽을 택했다 (2026-08-07 사용자 결정).
- isa-l은 **빌드 전제조건**이다: macOS `brew install isa-l`, linux는
  소스 빌드(v2.31.0 태그, `/usr/local` 설치). 데비안 `libisal-dev`는
  `.so`만 제공하고 정적 아카이브가 없어(bookworm·trixie 확인, 2026-08-07)
  정적 링크 요건을 만족하지 못한다.
- 배포 대상 linux는 `libisal.a` **정적 링크**로 prebuilt binary가 런타임
  라이브러리 의존 없이 동작하게 한다. 동적 링크는 "바이너리만 배포하면
  어디서든 실행"이라는 전제를 깨므로 채택하지 않는다.

## 결과

- DuckDB에 이어 두 번째 cgo 의존이 생겼다. 순수 Go 크로스 컴파일은 이미
  불가능했으므로 실질적 추가 비용은 빌드 호스트의 isa-l 설치뿐이다.
- 다코어 wall 하한(ADR 0001)의 상수가 해제 속도만큼 낮아진다
  (표준 샘플 Xeon 44s→11s).
