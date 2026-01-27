# 🏗️ Architecture Guide

**dlt-meta 기반 통합 데이터 파이프라인 - 아키텍처 상세**

---

## 📐 전체 아키텍처

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Unified Metadata (YAML)                          │
│               unified_pipeline_metadata.yml                         │
│  - RDB 연결 정보                                                      │
│  - Bronze/Silver 스키마                                              │
│  - 데이터 품질 규칙 (DQE)                                             │
│  - CDC 설정                                                          │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│           unified_metadata_processor.py                             │
│  자동 생성:                                                           │
│  ├── rdb_ingestion_*.json        (RDB → Delta)                      │
│  ├── onboard_dataflow_*.json     (Bronze DLT)                       │
│  ├── silver_transformations_*.json (Silver DLT)                     │
│  ├── dabs_*.yml                  (Databricks Jobs)                  │
│  ├── metadata_ddl_*.sql          (메타데이터 테이블)                  │
│  └── dqe/*.json                  (Expectation 규칙)                 │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Databricks Asset Bundles (DABs)                  │
│  ├── setup_metadata              (메타데이터 초기화)                  │
│  ├── create_mock_data            (테스트 데이터)                     │
│  ├── orchestrator_full_pipeline  (전체 파이프라인)                   │
│  ├── dlt_item_goods_option       (DLT 파이프라인)                    │
│  └── pipeline_monitoring         (자동 모니터링)                     │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Flow                                    │
│                                                                     │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐           │
│  │   RDB   │──▶│  Bronze │──▶│ Silver  │──▶│  Gold   │           │
│  │(MariaDB)│   │ (Delta) │   │(Delta)  │   │(Future) │           │
│  └─────────┘   └────┬────┘   └────┬────┘   └─────────┘           │
│                     │             │                                │
│                     ▼             ▼                                │
│              ┌──────────┐   ┌──────────┐                          │
│              │ Raw CDC  │   │Quarantine│                          │
│              │          │   │  (DQE)   │                          │
│              └──────────┘   └──────────┘                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 데이터 흐름 상세

### 1️⃣ **RDB Ingestion Layer**

**역할**: mssql → Delta Lake Raw 테이블

```python
RDB (mssql)
    ↓ [rdb_ingestion_runner.py]
    ↓ [JDBC Connection]
    ↓ [CDC Capture: S_OPT_NO, OPERATION_TYPE, INS_DATE]
    ↓
Raw Delta Table (item_goods_option_rdb_raw)
    - Partitioned by: ingest_dt
    - CDC 정보 포함: OPERATION_TYPE (I/U/D)
    - 메타데이터: _source_system, _ingestion_timestamp
```

**설정 파일**: `generated/rdb_ingestion_item_goods_option.json`

---

### 2️⃣ **Bronze Layer (DLT)**

**역할**: Raw → Bronze (dlt-meta 표준화)

```python
Raw Delta Table
    ↓ [DLT Pipeline: dlt-meta]
    ↓ [Auto Schema Detection]
    ↓ [Metadata Enrichment]
    ↓
Bronze Table (item_goods_option_bronze)
    - Format: Delta Lake
    - CDC: Change Data Feed enabled
    - Optimization: Liquid Clustering by OPT_NO
    - Features: Auto Optimize, Auto Compact
```

**설정 파일**: `generated/onboard_dataflow_item_goods_option.json`

**특징**:
- ✅ Schema Evolution 자동 처리
- ✅ Data Lineage 자동 기록
- ✅ Execution History 자동 저장

---

### 3️⃣ **Silver Layer (DLT + DQE)**

**역할**: Bronze → Silver (비즈니스 로직 + 데이터 품질)

```python
Bronze Table
    ↓ [DLT Pipeline: dlt-meta]
    ↓ [CDC Apply Changes: SCD Type 1]
    ↓ [Business Transformations]
    ↓ [Data Quality Expectations]
    ↓
    ├─▶ Silver Table (item_goods_option_silver)
    │   - 품질 통과 데이터
    │   - Active records only (deleted=false)
    │   
    └─▶ Quarantine Table (item_goods_option_silver_quarantine)
        - 품질 위반 데이터 (expect_or_fail)
        - Error message 포함
```

**설정 파일**: 
- `generated/silver_transformations_item_goods_option.json`
- `generated/dqe/silver_item_goods_option_expectations.json`

**CDC 설정**:
```json
{
  "keys": ["OPT_NO"],
  "sequence_by": "VERSION_CHG_DT",
  "scd_type": "1",
  "apply_as_deletes": "OPERATION_TYPE = 'D'",
  "except_column_list": ["OPERATION_TYPE", "SYNC_ID", "S_OPT_NO"]
}
```

**Data Quality Expectations**:
```json
{
  "expect": {
    "valid_opt_gd_no": "OPT_GD_NO IS NOT NULL"
  },
  "expect_or_fail": {
    "valid_opt_no": "OPT_NO IS NOT NULL"
  }
}
```

---

## 📊 메타데이터 관리

### **Metadata Schema 구조**

```sql
baikald1ws.metadata
├── bronze_dataflowspec          -- Bronze 파이프라인 설정
├── silver_dataflowspec          -- Silver 파이프라인 설정
├── pipeline_execution_history   -- 실행 이력 (모니터링용)
├── data_lineage                 -- 데이터 계보
├── metadata_versions            -- 메타데이터 버전 관리
└── monitoring_history           -- 모니터링 실행 이력
```

### **핵심 메타데이터 테이블**

#### 1. `pipeline_execution_history`
```sql
CREATE TABLE metadata.pipeline_execution_history (
  execution_id STRING,
  dataflow_id STRING,
  stage STRING,                    -- rdb_ingestion, bronze, silver
  status STRING,                   -- success, failed, running
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  rows_processed BIGINT,
  error_message STRING,
  metadata_json STRING
)
PARTITIONED BY (DATE(start_time));
```

**사용처**:
- ✅ 모니터링 (실패 감지, SLA 체크)
- ✅ 성능 분석 (duration, rows_processed)
- ✅ 이상치 탐지 (Z-Score 기반)

#### 2. `data_lineage`
```sql
CREATE TABLE metadata.data_lineage (
  lineage_id STRING,
  source_table STRING,
  target_table STRING,
  transformation_type STRING,
  execution_id STRING,
  created_at TIMESTAMP
);
```

#### 3. `monitoring_history`
```sql
CREATE TABLE metadata.monitoring_history (
  monitoring_timestamp TIMESTAMP,
  monitoring_date DATE,            -- 파티션
  monitoring_window_hours INT,
  alert_count INT,
  critical_count INT,
  warning_count INT,
  alerts_json STRING
)
PARTITIONED BY (monitoring_date);
```

---

## 🎯 모니터링 아키텍처

### **자동 모니터링 시스템**

```python
pipeline_monitor.py (Scheduled Job)
    ↓
    ├─ Check 1: 파이프라인 실패 감지
    │   SELECT * FROM pipeline_execution_history
    │   WHERE status = 'failed' AND start_time > NOW() - 1 HOUR
    │
    ├─ Check 2: 데이터 품질 이상치 (Z-Score)
    │   WITH stats AS (30일 평균/표준편차)
    │   SELECT * WHERE ABS(z_score) > 3
    │
    ├─ Check 3: SLA 초과
    │   SELECT * WHERE duration_seconds > SLA_LIMIT
    │
    ├─ Check 4: 성공률 추적
    │   SELECT success_rate FROM last_24_hours
    │
    └─ Check 5: 데이터 신선도
        SELECT MAX(start_time) WHERE status = 'success'
    ↓
Alerts
    ├─▶ Slack Webhook
    └─▶ Email (SMTP)
```

**통계적 방법**:
- **Z-Score Anomaly Detection**: `rows_processed`가 30일 평균 대비 3σ 이상 차이
- **SLA Monitoring**: Stage별 실행 시간 임계값
- **Success Rate**: 24시간 성공률 < 95% 시 알림

---

## 🔐 보안 아키텍처

### **Secrets 관리**

```yaml
Databricks Secrets (Scope: pipeline-cred-baikalx)
├── db_host              -- RDB 호스트
├── db_port              -- RDB 포트
├── db_username          -- RDB 사용자
├── db_password          -- RDB 비밀번호
├── db_database          -- RDB 데이터베이스
└── slack_webhook_url    -- Slack 알림 URL
```

**사용 예시**:
```python
db_host = dbutils.secrets.get("pipeline-cred-baikalx", "db_host")
```

### **Unity Catalog Permissions**

```sql
-- Catalog 권한
GRANT ALL PRIVILEGES ON CATALOG baikald1ws TO `data-engineering-team`;

-- Schema 권한
GRANT SELECT, MODIFY ON SCHEMA baikald1ws.sdp_poc TO `analytics-team`;
GRANT ALL PRIVILEGES ON SCHEMA baikald1ws.metadata TO `data-engineering-team`;

-- Service Principal (Production)
GRANT ALL PRIVILEGES ON CATALOG baikald1ws TO `dlt-meta-prod-sp`;
```

---

## 🚀 Databricks Asset Bundles (DABs)

### **Job 구조**

```yaml
resources:
  jobs:
    # 1. 메타데이터 초기화
    setup_metadata:
      - task: create_metadata_schemas (SQL)
    
    # 2. Mock 데이터 생성 (테스트)
    create_mock_data:
      - task: generate_mock_data (Python)
    
    append_bad_data:
      - task: append_bad_records (Python)
    
    # 3. DLT 파이프라인
    dlt_item_goods_option:
      - task: bronze_silver_pipeline (DLT)
    
    # 4. 전체 Orchestrator
    orchestrator_full_pipeline:
      - task: rdb_ingestion (Notebook)
      - task: dlt_bronze_silver (DLT) [depends_on: rdb_ingestion]
      - task: data_quality_validation (Notebook) [depends_on: dlt]
    
    # 5. 모니터링
    pipeline_monitoring:
      - task: run_monitoring_script (Notebook)
      schedule: "0 * * * *"  # 매시간
```

---

## 📈 확장성 고려사항

### **수평 확장**

1. **파티셔닝 전략**
   - Bronze/Silver: Liquid Clustering (OPT_NO)
   - Raw: Date Partitioning (ingest_dt)
   - Metadata: Date Partitioning (start_time, monitoring_date)

2. **Auto Scaling**
   - DLT: Enhanced Autoscaling 활성화
   - Jobs: Cluster 자동 조정 (min 1, max 8 workers)

3. **병렬 처리**
   - 여러 dataflow 동시 실행 가능
   - Task 병렬화 (orchestrator)

### **메타데이터 확장**

새로운 테이블 추가 시:

1. `unified_pipeline_metadata.yml`에 정의 추가
2. `unified_metadata_processor.py` 실행
3. `databricks bundle deploy`
4. 자동으로 모든 리소스 생성

---

## 🔄 CI/CD 통합

```yaml
# GitHub Actions / GitLab CI 예시
stages:
  - validate     # YAML 검증
  - generate     # 메타데이터 처리
  - deploy       # DABs 배포
  - test         # 통합 테스트

jobs:
  deploy:
    - python scripts/unified_metadata_processor.py
    - databricks bundle validate --target dev
    - databricks bundle deploy --target dev
    - databricks bundle run create_mock_data --target dev
    - databricks bundle run dlt_item_goods_option --target dev
```

---

## 💡 Best Practices

### 1. **메타데이터 버전 관리**
- ✅ `unified_pipeline_metadata.yml`을 Git으로 관리
- ✅ 변경 시 버전 태그 생성
- ✅ `metadata_versions` 테이블에 자동 기록

### 2. **모니터링**
- ✅ 매시간 자동 실행
- ✅ Slack 알림 활성화
- ✅ SLA 임계값 정기 검토

### 3. **데이터 품질**
- ✅ `expect_or_fail`는 신중하게 사용 (데이터 손실 가능)
- ✅ Quarantine 테이블 정기 검토
- ✅ 품질 규칙 점진적 강화

### 4. **성능 최적화**
- ✅ Liquid Clustering 활용
- ✅ Z-Order 인덱싱 (필요 시)
- ✅ Auto Compaction 활성화

---

## 📚 관련 문서

- **시작 가이드**: [START.md](./START.md)
- **프로덕션 체크리스트**: [CHECKLIST.md](./CHECKLIST.md)
- **프로젝트 개요**: [README.md](../README.md)

---

**이 아키텍처는 확장 가능하고, 유지보수하기 쉬우며, 프로덕션 레벨의 데이터 파이프라인을 제공합니다.** 🚀
