# 통합 메타데이터 아키텍처 (SSOT)

## 🏗️ 전체 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     SSOT (Single Source of Truth)                       │
│                                                                         │
│           metadata/unified_pipeline_metadata.yml                        │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ dataflows:                                                      │  │
│  │   - dataflow_id: "item_goods_option"                          │  │
│  │     source:        # RDB 소스 정의                             │  │
│  │     raw_bronze:    # RDB Ingestion 타겟                        │  │
│  │     bronze:        # dlt-meta Bronze                           │  │
│  │     silver:        # dlt-meta Silver + CDC                     │  │
│  └────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│               unified_metadata_processor.py                              │
│                                                                         │
│  Python 스크립트가 YAML을 읽어서 자동으로 생성:                          │
│  • RDB Ingestion Config (JSON)                                          │
│  • dlt-meta Onboarding (JSON)                                          │
│  • Silver Transformations (JSON)                                       │
│  • DABs Resources (YAML)                                               │
│  • Metadata Table DDL (SQL)                                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────┴───────────────────────────┐
        │                                                       │
        ↓                                                       ↓
┌──────────────────┐                                   ┌──────────────────┐
│  RDB Ingestion   │                                   │   dlt-meta       │
│  Job (Notebook)  │                                   │   Pipelines      │
│                  │                                   │                  │
│  • JDBC 연결     │                                   │  • Bronze        │
│  • prepare_query │                                   │  • Silver        │
│  • main_query    │                                   │  • CDC/SCD       │
│  • Secrets       │                                   │  • DQE           │
└──────────────────┘                                   └──────────────────┘
        ↓                                                       ↓
┌──────────────────┐                                   ┌──────────────────┐
│  Raw Bronze      │──────────────────────────────────→│  Bronze          │
│  Delta Table     │   dlt-meta reads from here        │  Delta Table     │
└──────────────────┘                                   └──────────────────┘
                                                                ↓
                                                       ┌──────────────────┐
                                                       │  Silver          │
                                                       │  Delta Table     │
                                                       └──────────────────┘
                                                                ↓
                                                       ┌──────────────────┐
                                                       │  Metadata Tables │
                                                       │  • Lineage       │
                                                       │  • Execution Log │
                                                       └──────────────────┘
```

---

## 📊 데이터 플로우

### Stage 0: 메타데이터 관리

```yaml
# unified_pipeline_metadata.yml (Git 버전 관리)
dataflow_id: "item_goods_option"
version: "1.0"
last_modified: "2025-01-26"
```

### Stage 1: RDB Extraction

```
SQL Server (gdevdb02)
    ↓
[prepare_query]
    ↓ CREATE #historytemp
[main_query]
    ↓ LEFT JOIN with #historytemp
[JDBC Read]
    ↓ Spark DataFrame
[Add Metadata]
    ↓ ingest_dt, _source_system, etc.
[Write Delta]
    ↓
Raw Bronze Table
(baikald1ws.sdp_poc.item_goods_option_rdb_raw)
```

### Stage 2: Bronze Processing (dlt-meta)

```
Raw Bronze Table
    ↓
[dlt-meta Bronze Pipeline]
    ↓ Filter: opt_gd_no IN (filter_table)
    ↓ Add: process_timestamp
    ↓ Partition: ingest_dt
[Write Delta]
    ↓
Bronze Table
(baikald1ws.sdp_poc.item_goods_option)
```

### Stage 3: Silver Processing (dlt-meta)

```
Bronze Table
    ↓
[dlt-meta Silver Pipeline]
    ↓ SELECT: business columns
    ↓ WHERE: date filters + validation
    ↓ TRANSFORM: CASE WHEN logic
    ↓ DEDUP: ROW_NUMBER() OVER (PARTITION BY...)
[CDC Apply Changes]
    ↓ SCD Type 1
    ↓ keys: [OPT_NO]
    ↓ sequence_by: SYNC_ID
    ↓ apply_as_deletes: is_del = 'Y'
[Write Delta]
    ↓
Silver Table
(baikald1ws.sdp_poc.item_goods_option_silver)
```

---

## 🔄 워크플로우 오케스트레이션

### 🧪 테스트 Job (test_pipeline_with_mock_data)

```
┌─────────────────────────────────────────────────┐
│  test_item_goods_option_with_mock_data          │
│  (Mock 데이터 기반 통합 테스트)                 │
└─────────────────────────────────────────────────┘
                      ↓
         ┌────────────┴────────────┐
         │                         │
         ↓                         ↓
┌──────────────────┐    ┌──────────────────────┐
│  Task 1:         │    │  Task 2:              │
│  Mock 데이터     │───→│  DLT Pipeline         │
│  생성            │    │  (Full Refresh)       │
└──────────────────┘    └──────────────────────┘
   (python script)             (pipeline)
         ↓                         ↓
   Raw Bronze           Bronze → Silver
   + Metadata                (CDC + DQE)
                               ↓
                    ┌──────────────────────┐
                    │  Task 3:              │
                    │  Data Quality         │
                    │  Validation           │
                    └──────────────────────┘
                           (notebook)
                               ↓
                       검증 리포트 생성
```

### 🚀 프로덕션 Job (orchestrator_full_pipeline)

```
┌─────────────────────────────────────────┐
│  orchestrator_item_goods_option_full    │
│  (실제 RDB 연동)                        │
└─────────────────────────────────────────┘
                  ↓
        ┌─────────┴─────────┐
        │                   │
        ↓                   ↓
┌──────────────┐    ┌──────────────────┐
│  Task 1:     │    │  Task 2:         │
│  RDB         │───→│  dlt-meta        │
│  Ingestion   │    │  Bronze/Silver   │
└──────────────┘    └──────────────────┘
   (notebook)         (pipeline, 증분)
        ↓                       ↓
   Raw Bronze ──────────→ Bronze → Silver
                (reads)        ↓
                    ┌──────────────────┐
                    │  Task 3:          │
                    │  Data Quality     │
                    │  Validation       │
                    └──────────────────┘
```

### 테스트 환경 실행 흐름

1. **수동 실행** (databricks bundle run)
2. **Task 1: Mock 데이터 생성** (~1분)
   - Script: `create_mock_data.py`
   - Output: Raw Bronze (70건) + Metadata 테이블
   - 특징: 7일치 CDC 시뮬레이션 데이터
3. **Task 2: DLT Pipeline (Full Refresh)** (~3-5분)
   - Pipeline: `dlt_item_goods_option`
   - Mode: `full_refresh: true` (테스트용)
   - Output: Bronze + Silver Delta
   - CDC: SCD Type 1 적용
   - DQE: expect_or_fail + expect 검증
4. **Task 3: Data Quality Validation** (~1분)
   - Notebook: `data_quality_validator.py`
   - 검증: 테이블 존재, 레코드 수, NULL, 중복
   - Output: JSON 리포트

### 프로덕션 환경 실행 흐름

1. **Scheduled Trigger** (cron: 0 2 * * *)
2. **Task 1: RDB Ingestion** (~5-10분)
   - Notebook 실행: `rdb_ingestion_runner.py`
   - Config: `generated/rdb_ingestion_item_goods_option.json`
   - Output: Raw Bronze Delta
   - 특징: JDBC 연결, Secrets 사용
3. **Task 2: DLT Bronze/Silver** (~10-15분)
   - Pipeline 실행: `dlt_item_goods_option`
   - Mode: `full_refresh: false` (증분 처리)
   - Output: Bronze + Silver Delta
   - 특징: CDC 증분 업데이트
4. **Task 3: Data Quality** (~1-2분)
   - Validation checks
   - Alert on failures

---

## 🗄️ 메타데이터 테이블 구조

### 1. pipeline_execution_history

```sql
CREATE TABLE metadata.pipeline_execution_history (
  execution_id STRING,
  dataflow_id STRING,
  stage STRING,           -- 'rdb_ingestion', 'bronze', 'silver'
  status STRING,          -- 'running', 'success', 'failed'
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  rows_processed BIGINT,
  error_message STRING,
  metadata MAP<STRING, STRING>
)
PARTITIONED BY (DATE(start_time));
```

### 2. data_lineage

```sql
CREATE TABLE metadata.data_lineage (
  lineage_id STRING,
  dataflow_id STRING,
  source_table STRING,    -- 'SQL Server: item..goods_option'
  target_table STRING,    -- 'baikald1ws.sdp_poc.item_goods_option_silver'
  transformation_logic STRING,
  execution_id STRING
);
```

### 3. metadata_versions

```sql
CREATE TABLE metadata.metadata_versions (
  version_id STRING,
  dataflow_id STRING,
  metadata_content STRING,  -- Full YAML content
  change_description STRING,
  created_by STRING,
  created_at TIMESTAMP,
  is_active BOOLEAN
);
```

---

## 🔐 보안 및 인증

### Databricks Secrets

```
Scope: db_credentials
├── gdevdb02_username  → SQL Server 사용자명
└── gdevdb02_password  → SQL Server 비밀번호
```

### 사용 위치

1. **RDB Ingestion Notebook**
   ```python
   username = dbutils.secrets.get("db_credentials", "gdevdb02_username")
   password = dbutils.secrets.get("db_credentials", "gdevdb02_password")
   ```

2. **Config 파일에서 참조**
   ```json
   {
     "connection": {
       "secrets": {
         "scope": "db_credentials",
         "username_key": "gdevdb02_username",
         "password_key": "gdevdb02_password"
       }
     }
   }
   ```

---

## 🌍 환경별 설정

### Development (dev)

```yaml
environments:
  dev:
    catalog: "baikald1ws_dev"
    schedule:
      enabled: false  # 수동 실행
    source:
      jdbc_url: "jdbc:sqlserver://gdevdb02-dev:1433"
```

### Production (prod)

```yaml
environments:
  prod:
    catalog: "baikald1ws"
    schedule:
      enabled: true  # 자동 실행
    source:
      jdbc_url: "jdbc:sqlserver://gdevdb02-prod:1433"
```

---

## 📈 모니터링 및 알림

### 실시간 모니터링 뷰

```sql
-- 최신 파이프라인 상태
SELECT * FROM metadata.v_latest_pipeline_status
WHERE dataflow_id = 'item_goods_option';

-- 전체 데이터 플로우
SELECT * FROM metadata.v_complete_dataflow
WHERE dataflow_id = 'item_goods_option';
```

### 알림 설정 (DABs)

```yaml
email_notifications:
  on_failure:
    - "data-platform-team@company.com"
  on_success:
    - "data-platform-team@company.com"  # prod only
```

---

## 🔧 확장 포인트

### 1. 새로운 소스 타입 추가

`unified_metadata_processor.py`에서:

```python
def generate_rdb_ingestion_config(self, dataflow):
    source_type = dataflow["source"]["type"]
    
    if source_type == "rdb_jdbc":
        # 기존 로직
    elif source_type == "oracle":
        # Oracle 지원 추가
    elif source_type == "postgresql":
        # PostgreSQL 지원 추가
```

### 2. 커스텀 변환 추가

```yaml
silver:
  transformations:
    custom_functions:
      - name: "encrypt_sensitive_columns"
        columns: ["SSN", "PHONE"]
      - name: "calculate_age"
        input: "BIRTH_DATE"
        output: "AGE"
```

### 3. 데이터 품질 검증 확장

```yaml
silver:
  data_quality:
    expectations:
      - name: "valid_opt_no"
        constraint: "OPT_NO IS NOT NULL"
        action: "fail"
      - name: "price_range"
        constraint: "OPT_PRICE >= 0 AND OPT_PRICE <= 1000000"
        action: "warn"
```

---

## 🎯 성능 최적화

### 1. JDBC 읽기 최적화

```yaml
extraction:
  read_options:
    fetchsize: "10000"     # 배치 크기
    numPartitions: "16"    # 병렬도
    partitionColumn: "OPT_NO"
    lowerBound: "1"
    upperBound: "1000000"
```

### 2. Delta 쓰기 최적화

```yaml
table_properties:
  delta.autoOptimize.optimizeWrite: "true"
  delta.autoOptimize.autoCompact: "true"
  delta.targetFileSize: "128MB"
```

### 3. DLT Pipeline 최적화

```yaml
clusters:
  - label: "default"
    num_workers: 4
    autoscale:
      min_workers: 2
      max_workers: 8
    spark_conf:
      spark.databricks.delta.optimizeWrite.enabled: "true"
```


