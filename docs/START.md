# 🚀 Quick Start Guide

**dlt-meta 기반 통합 데이터 파이프라인 - 빠른 시작**

이 가이드를 따라하면 **10분 내**에 전체 파이프라인을 실행할 수 있습니다.

---

## 📋 사전 요구사항

- ✅ Databricks Workspace 접근 권한
- ✅ Unity Catalog 생성 완료 (예: `baikald1ws`)
- ✅ Databricks CLI 설치 및 인증 완료
- ✅ Python 3.11+ 설치

---

## ⚡ 5분 빠른 시작

### Step 1: 저장소 클론 및 환경 설정

```bash
cd /Users/lina.ryoo/dev/dlt-meta/demo/dab_meta_gmarket

# Databricks CLI 인증 확인
databricks auth login --configure-cluster

# 메타데이터 설정 확인
cat metadata/unified_pipeline_metadata.yml
```

### Step 2: 메타데이터 기반 리소스 생성

```bash
# 통합 메타데이터 처리 (모든 설정 파일 자동 생성)
python3 scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev
```

**생성되는 파일**:
- `generated/rdb_ingestion_*.json` - RDB 연동 설정
- `generated/onboard_dataflow_*.json` - dlt-meta Bronze 설정
- `generated/silver_transformations_*.json` - Silver 변환 설정
- `generated/dabs_*.yml` - DABs 리소스 정의
- `generated/metadata_ddl_*.sql` - 메타데이터 테이블 DDL
- `generated/dqe/*.json` - 데이터 품질 규칙

### Step 3: Databricks에 배포

```bash
# DABs 배포
databricks bundle deploy --target dev
```

### Step 4: 메타데이터 테이블 초기화

```bash
# 메타데이터 스키마 및 테이블 생성
databricks bundle run setup_metadata --target dev
```

### Step 5A: Mock 데이터로 테스트 (RDB 없이)

```bash
# Mock 데이터 생성 (정상 + Bad Data)
databricks bundle run create_mock_data_with_bad_records --target dev

# DLT 파이프라인 실행 (Mock 데이터 기반)
databricks bundle run dlt_item_goods_option --target dev
```

### Step 5B: 실제 RDB 연동

```bash
# 전체 파이프라인 실행 (RDB → Bronze → Silver)
databricks bundle run orchestrator_item_goods_option_full --target dev
```

---

## 🎯 실행 결과 확인

### 1. **테이블 확인**

```sql
-- Bronze 테이블
SELECT * FROM baikald1ws.sdp_poc.item_goods_option_bronze LIMIT 10;

-- Silver 테이블
SELECT * FROM baikald1ws.sdp_poc.item_goods_option_silver LIMIT 10;

-- Quarantine 테이블 (데이터 품질 위반)
SELECT * FROM baikald1ws.sdp_poc.item_goods_option_silver_quarantine;
```

### 2. **파이프라인 실행 이력**

```sql
-- 실행 이력 확인
SELECT 
    dataflow_id,
    stage,
    status,
    rows_processed,
    duration_seconds,
    start_time
FROM baikald1ws.metadata.pipeline_execution_history
ORDER BY start_time DESC
LIMIT 10;
```

### 3. **데이터 품질 확인**

Databricks UI → **Delta Live Tables** → 파이프라인 선택 → **Expectations** 탭

---

## 📊 모니터링 시작

```bash
# 파이프라인 모니터링 실행 (1시간 윈도우)
databricks bundle run pipeline_monitoring --target dev

# 테스트 데이터로 실패 시뮬레이션
INSERT INTO baikald1ws.metadata.pipeline_execution_history (...)
VALUES (..., 'failed', ...);

# 모니터링 재실행 → Slack/Email 알림 확인
databricks bundle run pipeline_monitoring --target dev
```

---

## 🔧 트러블슈팅

### 문제 1: "Catalog not found"
```bash
# Unity Catalog 생성
databricks sql "CREATE CATALOG IF NOT EXISTS baikald1ws"
databricks sql "GRANT ALL PRIVILEGES ON CATALOG baikald1ws TO `your_user@company.com`"
```

### 문제 2: "Table already exists"
```bash
# 테이블 재생성
databricks sql "DROP TABLE IF EXISTS baikald1ws.sdp_poc.item_goods_option_bronze"
databricks bundle run setup_metadata --target dev
```

### 문제 3: "Secret not found"
```bash
# Secrets 설정
databricks secrets create-scope pipeline-cred-baikalx
databricks secrets put-secret pipeline-cred-baikalx db_host --string-value "your-db-host"
databricks secrets put-secret pipeline-cred-baikalx db_username --string-value "username"
databricks secrets put-secret pipeline-cred-baikalx db_password --string-value "password"
```

---

## 📚 다음 단계

1. **아키텍처 이해**: [ARCHITECTURE.md](./ARCHITECTURE.md)
2. **프로덕션 체크리스트**: [CHECKLIST.md](./CHECKLIST.md)
3. **상세 문서**: [README.md](../README.md)

---

## 🎉 성공 확인

다음이 모두 완료되면 성공입니다:

- [x] DABs 배포 완료
- [x] 메타데이터 테이블 생성 완료
- [x] DLT 파이프라인 실행 성공
- [x] Bronze/Silver 테이블에 데이터 존재
- [x] Expectation 작동 확인 (Quarantine 테이블)
- [x] 모니터링 Job 실행 성공

---

## 🆘 도움이 필요하신가요?

- **FAQ**: [README.md](../README.md) 하단 참조
- **프로덕션 가이드**: [CHECKLIST.md](./CHECKLIST.md)
- **팀 Slack**: #data-platform-support
- **Email**: data-platform-team@company.com

---

**축하합니다! 🎉 이제 dlt-meta 기반 통합 파이프라인을 성공적으로 실행했습니다!**
