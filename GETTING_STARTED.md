# 🚀 Getting Started - 5분 만에 시작하기

## 전체 과정

```
1. Secrets 설정 (1분)
   ↓
2. 메타데이터 작성 (2분)
   ↓
3. 설정 생성 (30초)
   ↓
4. 배포 (1분)
   ↓
5. 실행! (자동)
```

---

## Step 1: Secrets 설정 (1분)

```bash
# Scope 생성
databricks secrets create-scope pipeline-cred-baikalx

# 인증 정보 저장
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_username
# 프롬프트에서 username 입력

databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_password  
# 프롬프트에서 password 입력

# 확인
databricks secrets list --scope pipeline-cred-baikalx
```

✅ **완료!**

---

## Step 2: 메타데이터 확인 (2분)

`metadata/unified_pipeline_metadata.yml` 열기:

### 필수 확인 항목:

```yaml
# 1. JDBC URL (44번 줄)
jdbc_url: "jdbc:sqlserver://gdevdb02.baikalx.com:1433;database=item"
#                           ↑ 실제 서버 주소 확인!

# 2. Catalog 이름 (123, 144, 185번 줄)
catalog: "baikald1ws"
#        ↑ 실제 catalog 이름 확인!

# 3. Schema 이름 (124, 145, 186번 줄)
schema: "sdp_poc"
#       ↑ 실제 schema 이름 확인!

# 4. SQL 쿼리 (53-106번 줄)
prepare_query: |
  SELECT ... FROM itemsyncdb..s_goods_option  # ← 테이블명 확인!
  
main_query: |
  SELECT ... FROM item..goods_option  # ← 테이블명 확인!
```

✅ **완료!**

---

## Step 3: 설정 생성 (30초)

```bash
cd /Users/lina.ryoo/dev/dlt-meta/demo/dab_meta_gmarket
./run_demo.sh dev
```

**생성 확인:**
```bash
ls -l generated/
```

**예상 출력:**
```
generated/
├── rdb_ingestion_item_goods_option.json
├── onboarding_item_goods_option.json
├── transformations_item_goods_option.json
├── dabs_item_goods_option.yml
└── metadata_ddl_item_goods_option.sql
```

✅ **완료!**

---

## Step 4: 배포 (1분)

```bash
# 검증
databricks bundle validate --target dev

# 배포
databricks bundle deploy --target dev
```

**배포 확인:**
- Databricks Workspace → **Workflows** → Jobs 확인
  - `orchestrator_item_goods_option_full`
  - `rdb_ingestion_item_goods_option`
  - `setup_metadata`

- Databricks Workspace → **Workflows** → Delta Live Tables
  - `dlt_item_goods_option`

✅ **완료!**

---

## Step 5: 실행! (자동)

### 옵션 A: 전체 파이프라인 실행

```bash
databricks bundle run orchestrator_full_pipeline --target dev
```

**실행 순서:**
```
Task 1: RDB Ingestion (5-10분)
  → SQL Server 데이터 추출
  → Raw Bronze 테이블 생성
     ↓
Task 2: DLT Bronze/Silver (10-15분)
  → Bronze 테이블 생성 (필터링)
  → Silver 테이블 생성 (CDC)
     ↓
Task 3: Data Quality (1-2분)
  → 품질 검증
  → 리포트 생성
```

### 옵션 B: 개별 실행

```bash
# 1. 메타데이터 테이블만 생성
databricks bundle run setup_metadata --target dev

# 2. RDB Ingestion만 실행
databricks bundle run rdb_ingestion_item_goods_option --target dev

# 3. DLT Pipeline만 실행
databricks pipelines start-update <pipeline-id>
```

✅ **실행 완료!**

---

## 📊 결과 확인

### Databricks SQL에서 확인

```sql
-- Raw Bronze 확인
SELECT COUNT(*) as total_rows 
FROM baikald1ws.sdp_poc.item_goods_option_rdb_raw;

-- Bronze 확인
SELECT COUNT(*) as total_rows 
FROM baikald1ws.sdp_poc.item_goods_option_bronze;

-- Silver 확인
SELECT COUNT(*) as total_rows 
FROM baikald1ws.sdp_poc.item_goods_option_silver;

-- 실행 이력 확인
SELECT * FROM baikald1ws.metadata.pipeline_execution_history
WHERE dataflow_id = 'item_goods_option'
ORDER BY start_time DESC
LIMIT 5;
```

---

## 🎉 성공!

이제 다음을 완료했습니다:

✅ RDB → Raw Bronze → Bronze → Silver 파이프라인  
✅ 통합 메타데이터 (SSOT) 관리  
✅ 자동 설정 생성  
✅ DABs 오케스트레이션  
✅ 메타데이터 추적  

---

## 🔄 일상 작업

### 새 테이블 추가

```yaml
# metadata/unified_pipeline_metadata.yml에 추가
dataflows:
  - dataflow_id: "new_table"
    # ... 설정 복사 후 수정
```

```bash
./run_demo.sh dev
databricks bundle deploy --target dev
```

### 변환 로직 수정

```yaml
# unified_pipeline_metadata.yml 수정
silver:
  transformations:
    select_expressions:
      - "column1"
      - "UPPER(column2) as column2"  # ← 수정
```

```bash
./run_demo.sh dev
databricks bundle deploy --target dev
```

---

## 📚 다음 단계

- [CHECKLIST.md](CHECKLIST.md) - 상세 체크리스트
- [README.md](README.md) - 완전한 가이드
- [ARCHITECTURE.md](ARCHITECTURE.md) - 아키텍처 이해

---

**즐거운 데이터 파이프라인 구축 되세요! 🎊**
