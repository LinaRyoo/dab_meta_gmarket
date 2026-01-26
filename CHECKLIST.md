# ✅ 배포 전 체크리스트

## 📋 1. 환경 설정

### Databricks Workspace
- [ ] Workspace URL 확인
- [ ] Databricks CLI 인증: `databricks auth login`
- [ ] 적절한 권한 확인

### Unity Catalog
- [ ] Catalog 생성: `baikald1ws`
- [ ] Schema 생성 권한 확인

### Secrets
- [ ] Secrets scope 생성: `pipeline-cred-baikalx`
- [ ] Username secret 설정
- [ ] Password secret 설정

### ⚠️ Cloud Provider별 Instance Type

**현재 설정: AWS `i3.xlarge`**

- [ ] **databricks.yml에서 4곳의 node_type_id 확인**
  - AWS: `i3.xlarge`, `m5.xlarge`
  - Azure: `Standard_DS3_v2`, `Standard_E4s_v3`
  - GCP: `n1-standard-4`, `n2-standard-4`

- [ ] **확인 위치:**
  - Line 116: create_mock_data cluster
  - Line 162: orchestrator ingestion_cluster
  - Line 171: orchestrator validation_cluster
  - Line 231: test_pipeline cluster

### SQL Warehouse
- [ ] Warehouse ID 확인: `databricks sql-warehouses list`
- [ ] databricks.yml 49번 줄 업데이트

---

## 📝 2. 메타데이터 설정

`metadata/unified_pipeline_metadata.yml` 확인:

- [ ] JDBC URL 수정
- [ ] Secrets scope 이름 일치 확인
- [ ] Catalog/Schema 이름 확인
- [ ] SQL 쿼리 검증 (테이블명, 컬럼명)
- [ ] Parameters 확인 (start_date 등)

---

## 🔧 3. 설정 생성

```bash
./run_demo.sh dev
```

- [ ] 실행 성공
- [ ] `generated/` 폴더 생성 확인
- [ ] 6개 파일 생성 확인:
  - `rdb_ingestion_*.json`
  - `onboarding_*.json`
  - `transformations_*.json`
  - `dabs_*.yml`
  - `metadata_ddl_*.sql`
  - `dqe/*.json`

---

## 🔍 4. 네트워크 (RDB 사용 시만)

- [ ] RDB 서버 접근 가능 확인
- [ ] 방화벽/VPN 설정 확인
- [ ] JDBC 포트 확인 (SQL Server: 1433)

---

## 🎯 5. DABs 검증

```bash
databricks bundle validate --target dev
```

- [ ] Validation 성공
- [ ] generated/dabs_*.yml에서 instance type 확인

---

## 🚀 6. 배포

```bash
databricks bundle deploy --target dev
```

- [ ] 배포 성공
- [ ] Jobs 생성 확인:
  - `setup_metadata`
  - `create_mock_data`
  - `test_pipeline_with_mock_data`
  - `orchestrator_full_pipeline`
- [ ] Pipeline 생성: `dlt_item_goods_option`

---

## 🧪 7. 메타데이터 테이블 초기화

```bash
databricks bundle run setup_metadata --target dev
```

- [ ] 실행 성공
- [ ] 메타데이터 테이블 생성 확인:
  ```sql
  SHOW TABLES IN baikald1ws.metadata;
  ```

---

## 🎬 8. 테스트 실행

### Option A: Mock 데이터 (추천, 5-7분)

```bash
databricks bundle run test_pipeline_with_mock_data --target dev
```

- [ ] Task 1: Mock 데이터 생성 성공
- [ ] Task 2: DLT Pipeline 성공
- [ ] Task 3: Data Quality Validation 성공

### Option B: 실제 RDB (15-25분)

```bash
databricks bundle run orchestrator_full_pipeline --target dev
```

- [ ] Task 1: RDB Ingestion 성공
- [ ] Task 2: DLT Pipeline 성공
- [ ] Task 3: Validation 성공

---

## ✅ 9. 결과 확인

```sql
-- 테이블 생성 확인
SELECT COUNT(*) FROM baikald1ws.sdp_poc.item_goods_option_silver;

-- 데이터 품질 확인
SELECT 
  COUNT_IF(OPT_NO IS NULL) as null_opt_no,
  COUNT_IF(OPT_GD_NO IS NULL) as null_opt_gd_no
FROM baikald1ws.sdp_poc.item_goods_option_silver;
-- 예상: 모두 0
```

- [ ] Raw Bronze 테이블 생성
- [ ] Bronze 테이블 생성
- [ ] Silver 테이블 생성
- [ ] 데이터 품질 검증 통과

---

## 🐛 문제 해결

### Secrets 에러
```bash
databricks secrets list --scope pipeline-cred-baikalx
```

### generated/ 폴더 비어있음
```bash
./run_demo.sh dev
```

### DLT Pipeline 실패
- Databricks UI → Delta Live Tables → Logs 확인
- 컬럼 이름 오타 확인
- Metadata 테이블 존재 확인

### Instance Type 에러
- Cloud provider에 맞는 instance type인지 확인
- databricks.yml의 4곳 모두 수정

### RDB 연결 실패
- JDBC URL 확인
- Secrets 설정 확인
- 네트워크/방화벽 확인

---
