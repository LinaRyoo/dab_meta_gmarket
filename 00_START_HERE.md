# 👋 여기서 시작하세요!

## 📚 문서 가이드

이 프로젝트는 **통합 메타데이터 (SSOT)** 기반 데이터 파이프라인입니다.

### 🎯 목적에 맞는 문서 선택

| 상황 | 문서 |
|------|------|
| **처음 시작** | ► [GETTING_STARTED.md](GETTING_STARTED.md) - 5분 완성 |
| **빠른 참조** | ► [README.md](README.md) - 메인 가이드 |
| **단계별 체크** | ► [CHECKLIST.md](CHECKLIST.md) - 배포 체크리스트 |
| **상세 이해** | ► [ARCHITECTURE.md](ARCHITECTURE.md) - 아키텍처 |
| **고급 활용** | ► [README_UNIFIED_METADATA.md](README_UNIFIED_METADATA.md) - 완전 가이드 |
| **빠른 팁** | ► [QUICKSTART.md](QUICKSTART.md) - 팁 & 트릭 |

---

## 🚀 초고속 시작 (1분)

```bash
# 1. Secrets 설정
databricks secrets create-scope pipeline-cred-baikalx
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_username
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_password

# 2. 설정 생성
./run_demo.sh dev

# 3. 배포
databricks bundle deploy --target dev

# 4. 실행
databricks bundle run orchestrator_full_pipeline --target dev
```

---

## 📁 핵심 파일

### 🔧 수정이 필요한 파일 (필수)

1. **`metadata/unified_pipeline_metadata.yml`** - SSOT 메타데이터
   - JDBC URL 수정
   - Catalog/Schema 이름 확인
   - SQL 쿼리 수정

### 📝 참고용 파일 (읽기 전용)

- `databricks.yml` - DABs 설정 (자동 사용)
- `scripts/unified_metadata_processor.py` - 자동 생성 엔진
- `notebooks/*.py` - 실행 노트북 (자동 사용)

### 📊 자동 생성되는 파일 (Git ignore)

- `generated/` 폴더의 모든 파일

---

## 🎯 데이터 플로우

```
SQL Server
    ↓ [RDB Ingestion]
Raw Bronze (원본)
    ↓ [dlt-meta Bronze]
Bronze (필터링)
    ↓ [dlt-meta Silver]
Silver (CDC)
```

---

## ✨ 핵심 개념

### SSOT (Single Source of Truth)
- **하나의 YAML**로 전체 파이프라인 정의
- 중복 없이 일관성 보장

### 자동 생성
- 메타데이터 → 설정 파일 자동 생성
- 수동 작업 최소화

### 환경 분리
- dev/prod 자동 오버라이드
- 안전한 배포

---

## 🆘 도움이 필요하면

1. **[GETTING_STARTED.md](GETTING_STARTED.md)** 읽기 (5분 가이드)
2. **[CHECKLIST.md](CHECKLIST.md)** 따라하기 (단계별)
3. 문제 발생 시 → 해당 문서의 "문제 해결" 섹션 참고

---

**즐거운 데이터 파이프라인 구축! 🎊**
