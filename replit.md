# 프로젝트 정보

## 개요
단일 치료사를 위한 클라이언트 관리 시스템 - Python Flask 기반 웹 애플리케이션

## 최근 변경사항
- 2025-10-16: 프로젝트 초기 구축
  - Python Flask 웹 앱 구조 설정
  - CSV 기반 데이터 저장소 구현
  - 한글 UI 템플릿 작성 (로그인, 홈, 내담자, 회기)
  - 환경변수 기반 단일 사용자 인증 시스템

## 프로젝트 아키텍처

### 기술 스택
- **언어**: Python 3.11
- **프레임워크**: Flask 3.0.0
- **데이터**: Pandas (CSV 읽기/쓰기)
- **인증**: Flask Session + 환경변수

### 디렉토리 구조
```
/
├── main.py                 # Flask 앱 메인 파일
├── requirements.txt        # Python 의존성
├── templates/             # HTML 템플릿
│   ├── base.html          # 기본 레이아웃
│   ├── login.html         # 로그인 페이지
│   ├── home.html          # 홈/대시보드
│   ├── clients_list.html  # 내담자 목록
│   ├── client_form.html   # 내담자 추가 폼
│   ├── sessions_list.html # 회기 목록
│   └── session_form.html  # 회기 추가 폼
└── data/                  # CSV 데이터 저장소
    ├── clients.csv        # 내담자 정보
    └── sessions.csv       # 회기 기록
```

### 주요 기능
1. **인증 시스템**: SECRET_PASSWORD 환경변수 기반 로그인
2. **내담자 관리**: CRUD 기능, 이름/태그 검색
3. **회기 관리**: 내담자별 회기 기록 추가 및 조회
4. **데이터 내보내기**: CSV 파일 다운로드
5. **자동 ID 생성**: 타임스탬프 기반 고유 ID 생성

### 환경 변수
- `SECRET_PASSWORD`: 로그인 비밀번호 (필수)
- `SESSION_SECRET`: Flask 세션 암호화 키 (자동 생성)

## 사용자 선호사항
- 한글 UI 사용
- 깨끗하고 미니멀한 디자인 선호
- CSV 기반 간단한 데이터 관리 (MVP)

## 향후 개선 사항
- 내담자별 상세 페이지 및 수정/삭제 기능
- 회기 기록 필터링 (날짜 범위, 모드별)
- 통계 대시보드 (회기비 통계, 미납금 추적)
- 데이터 백업 및 복원 기능
- PostgreSQL 마이그레이션 고려
