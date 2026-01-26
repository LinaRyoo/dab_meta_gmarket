# 통합 메타데이터 기반 파이프라인 가이드 (SSOT)

## 📖 개요

이 프로젝트는 **Single Source of Truth (SSOT)** 패턴으로 전체 데이터 파이프라인의 메타데이터를 통합 관리합니다.

### 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│  unified_pipeline_metadata.yml (SSOT)                          │
│  - RDB Source 정의                                              │
│  - Bronze/Silver 정의                                           │
│  - 변환 로직 정의                                               │
│  - 스케줄 정의                                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  unified_metadata_processor.py                                  │
│  (메타데이터를 읽어서 각종 설정 자동 생성)                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
        ┌─────────────────────┼─────────────────────┐
        ↓                     ↓                     ↓
┌──────────────┐    ┌──────────────────┐    ┌─────────────┐
│ RDB          │    │ dlt-meta         │    │ DABs        │
│ Ingestion    │    │ Onboarding JSON  │    │ Resources   │
│ Config       │    │ + Transformations│    │ (Jobs/DLT)  │
└──────────────┘    └──────────────────┘    └─────────────┘
        ↓                     ↓                     ↓
    [실행]                [실행]                [배포]
```

### 데이터 플로우

```
SQL Server (RDB)
    ↓ [JDBC Extraction]
    ↓ (prepare_query + main_query)
    ↓
Raw Bronze (Delta)
    ↓ [dlt-meta Bronze Pipeline]
    ↓ (filtering + transformations)
    ↓
Bronze (Delta)
    ↓ [dlt-meta Silver Pipeline]
    ↓ (CDC SCD Type 1 + business logic)
    ↓
Silver (Delta)
```

---

## 🚀 빠른 시작

### 1. 통합 메타데이터 작성

`metadata/unified_pipeline_metadata.yml` 파일을 편집:

```yaml
dataflows:
  - dataflow_id: "your_table_name"
    dataflow_group: "your_group"
    
    source:
      type: "rdb_jdbc"
      connection:
        connection_name: "your_db"
        jdbc_url: "jdbc:sqlserver://..."
      extraction:
        prepare_query: |
          -- Your prepare query
        main_query: |
          -- Your main query
    
    raw_bronze:
      catalog: "your_catalog"
      schema: "your_schema"
      table: "your_table_raw"
    
    bronze:
      # ... bronze 설정
    
    silver:
      # ... silver 설정
```

### 2. 메타데이터 처리 및 설정 생성

```bash
# 개발 환경
python scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev

# 생성된 파일들:
# - generated/rdb_ingestion_*.json
# - generated/onboarding_*.json
# - generated/transformations_*.json
# - generated/dabs_*.yml
# - generated/metadata_ddl_*.sql
```

### 3. Databricks Secrets 설정

```bash
# Secrets scope 생성
databricks secrets create-scope db_credentials

# DB 인증 정보 저장
databricks secrets put-secret \
  --scope db_credentials \
  --key gdevdb02_username \
  --string-value "your_username"

databricks secrets put-secret \
  --scope db_credentials \
  --key gdevdb02_password \
  --string-value "your_password"
```

### 4. 메타데이터 테이블 초기화

```bash
# Databricks SQL에서 실행
databricks sql execute \
  --file generated/metadata_ddl_item_goods_option.sql
```

### 5. DABs 배포

```bash
# Validate
databricks bundle validate

# Deploy (dev)
databricks bundle deploy --target dev

# Deploy (prod)
databricks bundle deploy --target prod
```

### 6. 파이프라인 실행

```bash
# 개별 실행
databricks bundle run rdb_ingestion_item_goods_option --target dev
databricks bundle run dlt_item_goods_option --target dev

# 전체 오케스트레이션 실행
databricks bundle run orchestrator_item_goods_option_full --target dev
```

---

## 📁 디렉토리 구조

```
demo/dab_meta_gmarket/
├── metadata/
│   └── unified_pipeline_metadata.yml    # ⭐ SSOT 메타데이터
│
├── scripts/
│   └── unified_metadata_processor.py    # 메타데이터 처리 엔진
│
├── notebooks/
│   ├── rdb_ingestion_runner.py         # RDB 추출 노트북
│   ├── dlt_pipeline_runner.py          # DLT 파이프라인 노트북
│   └── data_quality_validator.py       # 데이터 품질 검증
│
├── generated/                           # 자동 생성된 파일들
│   ├── rdb_ingestion_*.json
│   ├── onboarding_*.json
│   ├── transformations_*.json
│   ├── dabs_*.yml
│   └── metadata_ddl_*.sql
│
├── databricks_unified.yml               # 통합 DABs 설정
└── README_UNIFIED_METADATA.md           # 이 문서
```

---

## 🎯 주요 기능

### 1. 통합 메타데이터 관리 (SSOT)

- ✅ RDB ingestion부터 Silver까지 **하나의 YAML**로 관리
- ✅ 환경별 설정 오버라이드 (dev/prod)
- ✅ 버전 관리 (Git)
- ✅ 중복 제거 및 일관성 보장

### 2. 자동 설정 생성

- ✅ RDB ingestion job config
- ✅ dlt-meta onboarding JSON
- ✅ Silver transformation JSON
- ✅ DABs resources (jobs, pipelines)
- ✅ 메타데이터 테이블 DDL

### 3. RDB 소스 지원

- ✅ SQL Server `prepare_query` + `main_query` 패턴
- ✅ JDBC 연결 풀 설정
- ✅ Databricks Secrets 통합
- ✅ 동적 파라미터 치환

### 4. 메타데이터 추적 (Lineage)

- ✅ 파이프라인 실행 이력
- ✅ 데이터 계보 추적
- ✅ 메타데이터 버전 관리
- ✅ 실시간 모니터링 뷰

### 5. Databricks Asset Bundles 통합

- ✅ 전체 파이프라인 오케스트레이션
- ✅ 환경별 배포 (dev/prod)
- ✅ 의존성 관리
- ✅ 스케줄링 및 알림

---

## 📊 메타데이터 테이블

자동 생성되는 메타데이터 테이블:

### 1. `pipeline_execution_history`

파이프라인 실행 이력 추적

```sql
SELECT * 
FROM baikald1ws.metadata.pipeline_execution_history
WHERE dataflow_id = 'item_goods_option'
ORDER BY start_time DESC
LIMIT 10;
```

### 2. `data_lineage`

데이터 계보 추적

```sql
SELECT 
  source_table,
  target_table,
  transformation_logic
FROM baikald1ws.metadata.data_lineage
WHERE dataflow_id = 'item_goods_option';
```

### 3. `metadata_versions`

메타데이터 변경 이력

```sql
SELECT 
  version_id,
  change_description,
  created_by,
  created_at
FROM baikald1ws.metadata.metadata_versions
WHERE dataflow_id = 'item_goods_option'
  AND is_active = true;
```

---

## 🔧 고급 사용법

### 1. 새로운 데이터플로우 추가

```yaml
# metadata/unified_pipeline_metadata.yml에 추가
dataflows:
  - dataflow_id: "new_table"
    # ... 설정
```

```bash
# 재생성
python scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev

# 재배포
databricks bundle deploy --target dev
```

### 2. 변환 로직 수정

```yaml
# unified_pipeline_metadata.yml의 silver.transformations 수정
silver:
  transformations:
    select_expressions:
      - "column1"
      - "CASE WHEN ... END as column2"
    where_clauses:
      - "column1 IS NOT NULL"
```

### 3. 스케줄 변경

```yaml
# unified_pipeline_metadata.yml
schedule:
  cron: "0 4 * * *"  # 매일 새벽 4시
  timezone: "Asia/Seoul"
```

### 4. 환경별 다른 설정

```yaml
environments:
  dev:
    catalog: "baikald1ws_dev"
    schedule:
      enabled: false  # dev는 수동 실행
  
  prod:
    catalog: "baikald1ws"
    schedule:
      enabled: true   # prod는 자동 실행
```

---

## 🐛 트러블슈팅

### JDBC 연결 실패

```bash
# Secrets 확인
databricks secrets list --scope db_credentials

# 연결 테스트
databricks jobs run-now --job-id <job_id>
```

### 메타데이터 테이블 없음

```bash
# DDL 재실행
databricks sql execute \
  --file generated/metadata_ddl_*.sql
```

### 설정 변경이 반영 안됨

```bash
# 1. 재생성
python scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev

# 2. 재배포
databricks bundle deploy --target dev --force
```

---

## 📚 참고 자료

- [dlt-meta Documentation](https://databrickslabs.github.io/dlt-meta/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/)

---

## 🤝 기여

메타데이터 스키마 개선, 새로운 소스 타입 지원 등 기여를 환영합니다!

---

## 📝 변경 이력

### v1.0.0 (2025-01-26)
- ✨ 초기 버전: 통합 메타데이터 SSOT 패턴 구현
- ✨ RDB (SQL Server) 소스 지원
- ✨ dlt-meta 통합
- ✨ DABs 오케스트레이션
- ✨ 메타데이터 lineage 추적
