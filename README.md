# 🚀 통합 메타데이터 기반 데이터 파이프라인

RDB ingestion부터 dlt-meta Bronze/Silver까지 **하나의 YAML**로 관리하는 완전한 솔루션

---

## 🎯 빠른 시작

### Mock 테스트 (RDB 연결 불필요)

```bash
./run_demo.sh dev
databricks bundle deploy --target dev
databricks bundle run setup_metadata --target dev
databricks bundle run test_pipeline_with_mock_data --target dev
```

### 프로덕션 (실제 RDB)

```bash
# 1. Secrets 설정
databricks secrets create-scope pipeline-cred-baikalx
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_username
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_password

# 2. 메타데이터 확인 및 수정
vi metadata/unified_pipeline_metadata.yml

# 3. 실행
./run_demo.sh dev
databricks bundle deploy --target dev
databricks bundle run orchestrator_full_pipeline --target dev
```

---

## ✨ 주요 특징

| 특징 | 설명 |
|------|------|
| **SSOT** | 하나의 YAML로 전체 관리 |
| **자동화** | 설정 파일 자동 생성 |
| **Mock 테스트** | RDB 없이 전체 파이프라인 테스트 |
| **CDC** | Change Data Capture 지원 |
| **DQE** | Data Quality Expectations 통합 |
| **환경 분리** | dev/prod 자동 오버라이드 |

---

## 📁 디렉토리 구조

```
dab_meta_gmarket/
├── metadata/
│   └── unified_pipeline_metadata.yml    # ⭐ SSOT
│
├── scripts/
│   ├── unified_metadata_processor.py    # 자동 생성 엔진
│   └── create_mock_data.py              # Mock 데이터 생성
│
├── notebooks/
│   ├── rdb_ingestion_runner.py          # RDB 추출
│   ├── dlt_pipeline_runner.py           # DLT 실행
│   └── data_quality_validator.py        # 품질 검증
│
├── src/                                 # dlt-meta 소스
│   ├── dataflow_spec.py                 # Bronze/Silver Spec
│   ├── dataflow_pipeline.py             # DLT 파이프라인 로직
│   └── pipeline_readers.py              # 메타데이터 읽기
│
├── generated/                           # 자동 생성 (배포 시)
│   ├── rdb_ingestion_*.json
│   ├── onboarding_*.json
│   ├── transformations_*.json
│   ├── dabs_*.yml
│   └── dqe/*.json
│
├── databricks.yml                       # DABs 메인 설정
└── run_demo.sh                          # 원클릭 실행
```

---

## 🔧 메타데이터 구조

`metadata/unified_pipeline_metadata.yml` (SSOT):

```yaml
dataflows:
  - dataflow_id: "item_goods_option"
    dataflow_group: "item"
    
    source:                              # RDB 설정
      type: "rdb_jdbc"
      connection:
        jdbc_url: "jdbc:sqlserver://..."
      extraction:
        main_query: "SELECT * FROM..."
    
    bronze:                              # Bronze Layer
      catalog: "baikald1ws"
      schema: "sdp_poc"
      table: "item_goods_option_raw"
      transformations:
        add_columns:
          - name: "ingest_dt"
            expr: "current_date()"
    
    silver:                              # Silver Layer
      catalog: "baikald1ws"
      schema: "sdp_poc"
      table: "item_goods_option_silver"
      cdc_apply_changes:                 # CDC 설정
        keys: ["OPT_NO"]
        sequence_by: "SYNC_ID"
        scd_type: 2
      data_quality:                      # DQE 설정
        expectations:
          - name: "valid_opt_no"
            constraint: "OPT_NO IS NOT NULL"
            action: "fail"
```

---

## 🔄 실행 흐름

### 1. Mock 테스트 Job (`test_pipeline_with_mock_data`)

```
Task 1: generate_mock_data
  → 70건 생성 (7일 × 10건/일)
  ↓
Task 2: run_dlt_pipeline (full_refresh)
  → Bronze: 필터링 + 변환
  → Silver: CDC + DQE
  ↓
Task 3: validate_data_quality
  → 품질 검증 리포트
```

### 2. 프로덕션 Job (`orchestrator_full_pipeline`)

```
Task 1: ingest_rdb
  → RDB 데이터 추출
  ↓
Task 2: run_dlt_pipeline (incremental)
  → Bronze: 필터링 + 변환
  → Silver: CDC + DQE
  ↓
Task 3: validate_data_quality
  → 품질 검증 리포트
```

---

## 🛠️ 고급 사용법

### 메타데이터 재생성

```bash
./run_demo.sh dev
```

자동 생성:
- `generated/rdb_ingestion_*.json`
- `generated/onboarding_*.json`
- `generated/transformations_*.json`
- `generated/dabs_*.yml`
- `generated/dqe/*.json`

### 새 테이블 추가

1. `metadata/unified_pipeline_metadata.yml`에 dataflow 추가
2. `./run_demo.sh dev` 실행
3. `databricks bundle deploy --target dev`
4. Job 실행

### DQE 추가

```yaml
silver:
  data_quality:
    expectations:
      - name: "valid_price"
        constraint: "OPT_PRICE >= 0"
        action: "drop"          # drop/fail/warn/quarantine
```

### CDC 설정

```yaml
silver:
  cdc_apply_changes:
    keys: ["OPT_NO"]           # Primary Key
    sequence_by: "SYNC_ID"     # 순서 결정 컬럼
    scd_type: 2                # SCD Type 2
    track_history_except_column_list:
      - "SYNC_ID"
      - "_ingestion_timestamp"
```

---

## 🐛 문제 해결

### Secrets 에러
```bash
databricks secrets list --scope pipeline-cred-baikalx
databricks secrets list-scopes
```

### Instance Type 에러
- AWS: `i3.xlarge`, `r5.large`
- Azure: `Standard_DS3_v2`, `Standard_E4s_v3`
- GCP: `n1-standard-4`, `n2-standard-4`

### DLT 실패
- UI → Delta Live Tables → Event Log 확인
- `full_refresh: true`로 재실행

### Mock 데이터 재생성
```bash
databricks bundle run create_mock_data --target dev
```

---

## 📊 데이터 확인

```sql
-- Bronze 테이블
SELECT COUNT(*) FROM baikald1ws.sdp_poc.item_goods_option_raw;

-- Silver 테이블
SELECT COUNT(*) FROM baikald1ws.sdp_poc.item_goods_option_silver;

-- 데이터 품질 확인
SELECT 
  COUNT_IF(OPT_NO IS NULL) as null_opt_no,
  COUNT_IF(OPT_GD_NO IS NULL) as null_opt_gd_no
FROM baikald1ws.sdp_poc.item_goods_option_silver;

-- CDC 확인 (SCD Type 2)
SELECT OPT_NO, __START_AT, __END_AT, is_del 
FROM baikald1ws.sdp_poc.item_goods_option_silver
WHERE OPT_NO = 12345
ORDER BY __START_AT;
```



