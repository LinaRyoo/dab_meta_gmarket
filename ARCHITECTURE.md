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

### DABs Orchestrator Job

```
┌─────────────────────────────────────────┐
│  orchestrator_item_goods_option_full    │
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
   (notebook)              (pipeline)
        ↓                       ↓
   Raw Bronze ──────────→ Bronze → Silver
                (reads)
```

### 실행 흐름

1. **Scheduled Trigger** (cron: 0 2 * * *)
2. **Task 1: RDB Ingestion**
   - Notebook 실행: `rdb_ingestion_runner.py`
   - Config: `generated/rdb_ingestion_item_goods_option.json`
   - Output: Raw Bronze Delta
   - Duration: ~5-10분
3. **Task 2: DLT Bronze/Silver** (depends_on: Task 1)
   - Pipeline 실행: `dlt_item_goods_option`
   - Config: dlt-meta onboarding JSON
   - Output: Bronze + Silver Delta
   - Duration: ~10-15분
4. **Task 3: Data Quality** (optional)
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

---

## 📚 참고 문서

- [Unified Metadata Schema](metadata/unified_pipeline_metadata.yml)
- [Processor Implementation](scripts/unified_metadata_processor.py)
- [RDB Ingestion Runner](notebooks/rdb_ingestion_runner.py)
- [Quick Start Guide](QUICKSTART.md)
- [Full Documentation](README_UNIFIED_METADATA.md)
