# 치료사 클라이언트 관리 시스템

단일 치료사를 위한 클라이언트 회기 이력 관리 웹 애플리케이션입니다.

## 기능

- **내담자 관리**: 내담자 정보 추가, 조회, 검색 (이름/태그)
- **회기 관리**: 회기 기록 추가, 조회 (날짜, 시간, 목표, 중재 등)
- **데이터 내보내기**: CSV 파일로 내담자 및 회기 데이터 다운로드
- **간편한 로그인**: 단일 사용자 비밀번호 인증
- **한글 UI**: 깨끗하고 직관적인 한국어 인터페이스

## 설치 및 실행

### 1. 환경 설정

Replit Secrets에 `SECRET_PASSWORD` 환경변수를 추가하세요:
- 왼쪽 사이드바에서 "Secrets" (자물쇠 아이콘) 클릭
- Key: `SECRET_PASSWORD`
- Value: 원하는 비밀번호 입력

### 2. 의존성 설치

```bash
pip install -r requirements.txt
```

### 3. 앱 실행

```bash
python main.py
```

앱이 `http://0.0.0.0:5000`에서 실행됩니다.

## 사용 방법

1. **로그인**: SECRET_PASSWORD로 설정한 비밀번호로 로그인
2. **내담자 추가**: "내담자" > "새 내담자 추가" 버튼 클릭
3. **회기 기록**: "회기" > "새 회기 추가" 버튼 클릭하여 내담자별 회기 기록
4. **데이터 내보내기**: 상단 메뉴에서 "클라이언트 CSV" 또는 "회기 CSV" 클릭

## 데이터 구조

### clients.csv
- client_id: 내담자 고유 ID (자동 생성)
- name: 이름
- phone: 전화번호
- email: 이메일
- birth_year: 출생연도
- gender: 성별
- first_session_date: 첫 회기 날짜
- status: 상태 (active/inactive)
- tags: 태그 (쉼표로 구분)
- notes: 메모

### sessions.csv
- session_id: 회기 고유 ID (자동 생성)
- client_id: 내담자 ID (외래키)
- date: 회기 날짜
- duration_minutes: 시간(분)
- mode: 모드 (대면/온라인)
- goals: 목표
- interventions: 중재/개입
- notes: 회기 노트
- next_actions: 다음 단계
- next_session_date: 다음 회기 날짜
- fee: 비용
- paid: 납부 여부 (Y/N)
- rating: 평가 (1-5)

## 기술 스택

- **Backend**: Python 3.11 + Flask
- **Data**: Pandas + CSV
- **Frontend**: HTML5 + CSS3
- **인코딩**: UTF-8 (한글 완벽 지원)

## 주의사항

- 이 앱은 MVP(최소 기능 제품)로 CSV 파일 기반입니다.
- 프로덕션 환경에서는 PostgreSQL 등의 데이터베이스 사용을 권장합니다.
- 정기적으로 CSV 파일을 백업하세요.
