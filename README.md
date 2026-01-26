# 🚀 통합 메타데이터 기반 데이터 파이프라인 (SSOT)

RDB ingestion부터 dlt-meta Bronze/Silver까지 **하나의 메타데이터**로 관리하는 완전한 솔루션입니다.

## 📋 목차

- [빠른 시작](#-빠른-시작)
- [주요 특징](#-주요-특징)
- [디렉토리 구조](#-디렉토리-구조)
- [상세 가이드](#-상세-가이드)
- [아키텍처](#-아키텍처)
- [문제 해결](#-문제-해결)

---

## 🎯 빠른 시작

### 1. Secrets 설정

```bash
databricks secrets create-scope pipeline-cred-baikalx
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_username
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_password
```

### 2. 메타데이터 작성

`metadata/unified_pipeline_metadata.yml` 편집:

```yaml
dataflows:
  - dataflow_id: "item_goods_option"
    source:
      type: "rdb_jdbc"
      connection:
        jdbc_url: "jdbc:sqlserver://your-server:1433;database=item"
      extraction:
        prepare_query: |
          SELECT * INTO #temp FROM source_table
        main_query: |
          SELECT * FROM #temp
```

### 3. 설정 자동 생성

```bash
./run_demo.sh dev
```

**생성되는 파일:**
- `generated/rdb_ingestion_*.json` - RDB 추출 설정
- `generated/onboarding_*.json` - dlt-meta 설정
- `generated/transformations_*.json` - Silver 변환 로직
- `generated/dabs_*.yml` - DABs 리소스
- `generated/metadata_ddl_*.sql` - 메타데이터 테이블

### 4. 배포 및 실행

```bash
# 검증
databricks bundle validate --target dev

# 배포
databricks bundle deploy --target dev

# 실행
databricks bundle run orchestrator_full_pipeline --target dev
```

---

## ✨ 주요 특징

### 🎯 SSOT (Single Source of Truth)
- ✅ 하나의 YAML로 RDB ingestion + Bronze + Silver 관리
- ✅ 중복 제거 및 일관성 보장
- ✅ Git으로 버전 관리

### 🤖 완전 자동화
- ✅ 설정 파일 자동 생성
- ✅ DABs 리소스 자동 생성
- ✅ 메타데이터 테이블 자동 생성

### 🌍 환경 분리
- ✅ dev/prod 자동 오버라이드
- ✅ 안전한 배포 프로세스

### 🔍 메타데이터 추적
- ✅ 파이프라인 실행 이력
- ✅ 데이터 계보 (lineage)
- ✅ 버전 관리

---

## 📁 디렉토리 구조

```
demo/dab_meta_gmarket/
├── metadata/
│   └── unified_pipeline_metadata.yml    # ⭐ SSOT - 모든 메타데이터
│
├── scripts/
│   └── unified_metadata_processor.py    # 자동 생성 엔진
│
├── notebooks/
│   ├── rdb_ingestion_runner.py         # RDB 추출 실행
│   ├── dlt_pipeline_runner.py          # DLT 파이프라인 실행
│   └── data_quality_validator.py       # 데이터 품질 검증
│
├── generated/                           # 자동 생성 (git ignore)
│   ├── rdb_ingestion_*.json
│   ├── onboarding_*.json
│   ├── transformations_*.json
│   ├── dabs_*.yml
│   └── metadata_ddl_*.sql
│
├── databricks.yml                       # DABs 메인 설정
├── run_demo.sh                         # 원클릭 실행 스크립트
├── .gitignore                          # Git 설정
│
└── README.md                           # 이 문서
```

---

## 📖 상세 가이드

### 데이터 플로우

```
SQL Server (RDB)
    ↓ [JDBC Extraction]
    ↓ (prepare_query + main_query)
Raw Bronze (Delta)
    ↓ [dlt-meta Bronze Pipeline]
    ↓ (filtering + transformations)
Bronze (Delta)
    ↓ [dlt-meta Silver Pipeline]
    ↓ (CDC SCD Type 1 + business logic)
Silver (Delta)
```

### 메타데이터 구조

#### 1. Source (RDB)
```yaml
source:
  type: "rdb_jdbc"
  connection:
    connection_name: "gdevdb02"
    jdbc_url: "jdbc:sqlserver://..."
    secrets:
      scope: "pipeline-cred-baikalx"
  extraction:
    prepare_query: |
      -- SQL Server temp table 생성
    main_query: |
      -- 데이터 추출
```

#### 2. Raw Bronze
```yaml
raw_bronze:
  catalog: "baikald1ws"
  schema: "sdp_poc"
  table: "item_goods_option_rdb_raw"
  format: "delta"
  metadata_columns:
    ingest_dt: "${trigger_time.isoformat()}"
    _source_system: "gdevdb02"
```

#### 3. Bronze
```yaml
bronze:
  catalog: "baikald1ws"
  schema: "sdp_poc"
  table: "item_goods_option_bronze"
  source_format: "delta"
  source_reference: "raw_bronze"
  transformations:
    - type: "filter"
      condition: "opt_gd_no IN (...)"
```

#### 4. Silver
```yaml
silver:
  catalog: "baikald1ws"
  schema: "sdp_poc"
  table: "item_goods_option_silver"
  cdc_apply_changes:
    keys: ["OPT_NO"]
    sequence_by: "SYNC_ID"
    scd_type: "1"
```

### 환경별 설정

```yaml
environments:
  dev:
    catalog: "baikald1ws"
    schedule:
      enabled: false  # 수동 실행
  
  prod:
    catalog: "baikald1ws"
    schedule:
      enabled: true   # 자동 실행
```

---

## 🏗️ 아키텍처

### 전체 아키텍처

```
┌─────────────────────────────────────────┐
│  unified_pipeline_metadata.yml (SSOT)  │
└─────────────────────────────────────────┘
                ↓
┌─────────────────────────────────────────┐
│  unified_metadata_processor.py          │
└─────────────────────────────────────────┘
                ↓
    ┌───────────┴───────────┐
    ↓                       ↓
RDB Config          dlt-meta Onboarding
    ↓                       ↓
[Execution]            [Execution]
```

### 실행 흐름

```
Task 1: RDB Ingestion
  └─ rdb_ingestion_runner.py
     └─ SQL Server → Raw Bronze

Task 2: DLT Bronze/Silver
  └─ dlt_pipeline_runner.py
     └─ Raw Bronze → Bronze → Silver

Task 3: Data Quality
  └─ data_quality_validator.py
     └─ 품질 검증 및 리포트
```

### 메타데이터 테이블

자동 생성되는 메타데이터 테이블:

1. **pipeline_execution_history** - 파이프라인 실행 이력
2. **data_lineage** - 데이터 계보 추적
3. **metadata_versions** - 메타데이터 버전 관리

---

## 🔧 문제 해결

### Q: Secrets 에러 발생

```bash
# Scope 확인
databricks secrets list-scopes

# Secret 확인
databricks secrets list --scope pipeline-cred-baikalx

# 재설정
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_username
```

### Q: generated/ 폴더가 비어있음

```bash
# 메타데이터 재생성
./run_demo.sh dev

# 또는 수동 실행
python scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev
```

### Q: DLT Pipeline 실행 실패

```bash
# 로그 확인
databricks pipelines get <pipeline-id> --output json | jq '.latest_updates[0]'

# dlt-meta 라이브러리 확인
# DLT Pipeline 설정 → Libraries → dlt-meta>=0.0.10 확인
```

### Q: RDB 연결 실패

메타데이터 파일에서 JDBC URL 확인:

```yaml
source:
  connection:
    jdbc_url: "jdbc:sqlserver://gdevdb02.baikalx.com:1433;database=item"
```

---

## 📚 추가 문서

- **상세 가이드**: [README_UNIFIED_METADATA.md](README_UNIFIED_METADATA.md)
- **빠른 시작**: [QUICKSTART.md](QUICKSTART.md)
- **아키텍처**: [ARCHITECTURE.md](ARCHITECTURE.md)

---

## 🤝 기여 및 지원

이 프로젝트는 dlt-meta 프레임워크를 확장한 것입니다.

- [dlt-meta Documentation](https://databrickslabs.github.io/dlt-meta/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)

---

## 📝 변경 이력

### v1.0.0 (2026-01-26)
- ✨ 통합 메타데이터 SSOT 패턴 구현
- ✨ RDB (SQL Server) 소스 지원
- ✨ dlt-meta 통합
- ✨ DABs 오케스트레이션
- ✨ Lakeflow SDP (dp) API 지원
- ✨ 메타데이터 lineage 추적
- ✨ Serverless SQL Warehouse 지원

---

**🎉 이제 SSOT로 메타데이터를 관리하세요!**
