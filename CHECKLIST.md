# ✅ 배포 전 체크리스트

이 체크리스트를 따라 배포 전 모든 항목을 확인하세요.

## 📋 1. 환경 설정

### Databricks Workspace
- [ ] Workspace URL 확인: `https://e2-demo-field-eng.cloud.databricks.com/`
- [ ] Databricks CLI 인증 완료: `databricks auth login`
- [ ] 적절한 권한 확인 (Workspace Admin 또는 적절한 권한)

### Unity Catalog
- [ ] Catalog 생성 확인: `baikald1ws`
- [ ] Schema 생성 권한 확인
- [ ] Volume 생성 권한 확인

### Secrets
```bash
# Secrets Scope 생성
databricks secrets create-scope pipeline-cred-baikalx

# DB 인증 정보 저장
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_username
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_password

# 확인
databricks secrets list --scope pipeline-cred-baikalx
```
- [ ] Secrets scope 생성 완료: `pipeline-cred-baikalx`
- [ ] Username secret 설정 완료
- [ ] Password secret 설정 완료

---

## 📝 2. 메타데이터 설정

### `metadata/unified_pipeline_metadata.yml` 확인

- [ ] **JDBC URL** 수정:
  ```yaml
  jdbc_url: "jdbc:sqlserver://gdevdb02.baikalx.com:1433;database=item"
  ```

- [ ] **Secrets scope** 확인:
  ```yaml
  secrets:
    scope: "pipeline-cred-baikalx"  # ← 실제 scope 이름
  ```

- [ ] **Catalog 이름** 확인:
  ```yaml
  catalog: "baikald1ws"  # ← 실제 catalog 이름
  ```

- [ ] **SQL 쿼리** 검증:
  - prepare_query 문법 확인
  - main_query 테이블명 확인
  - 컬럼명 확인

- [ ] **파라미터** 확인:
  ```yaml
  parameters:
    start_date:
      default: "2025-06-10"  # ← 실제 시작 날짜
  ```

---

## 🔧 3. 설정 생성

```bash
cd /Users/lina.ryoo/dev/dlt-meta/demo/dab_meta_gmarket
./run_demo.sh dev
```

- [ ] 스크립트 실행 성공
- [ ] `generated/` 폴더 생성 확인
- [ ] 다음 파일들 생성 확인:
  - [ ] `generated/rdb_ingestion_item_goods_option.json`
  - [ ] `generated/onboarding_item_goods_option.json`
  - [ ] `generated/transformations_item_goods_option.json`
  - [ ] `generated/dabs_item_goods_option.yml`
  - [ ] `generated/metadata_ddl_item_goods_option.sql`

---

## 🎯 4. DABs 검증

```bash
databricks bundle validate --target dev
```

- [ ] Validation 성공 (no errors)
- [ ] Workspace path 확인
- [ ] Resources 정의 확인

---

## 🚀 5. 배포

```bash
databricks bundle deploy --target dev
```

**생성되는 리소스:**
- [ ] Job: `setup_metadata`
- [ ] Job: `orchestrator_full_pipeline`
- [ ] Job: `rdb_ingestion_item_goods_option` (개별)
- [ ] Pipeline: `dlt_item_goods_option`

**확인:**
- [ ] Databricks Workspace에서 Jobs 확인
- [ ] Databricks Workspace에서 Pipelines 확인
- [ ] 노트북 업로드 확인

---

## 🧪 6. 테스트 실행

### 6.1. 메타데이터 테이블 생성 (선택)

```bash
databricks bundle run setup_metadata --target dev
```

- [ ] SQL 실행 성공
- [ ] 메타데이터 테이블 생성 확인:
  - [ ] `baikald1ws.metadata.pipeline_execution_history`
  - [ ] `baikald1ws.metadata.data_lineage`
  - [ ] `baikald1ws.metadata.metadata_versions`

### 6.2. 전체 파이프라인 실행

```bash
databricks bundle run orchestrator_full_pipeline --target dev
```

**실행 순서:**
1. Task 1: RDB Ingestion
   - [ ] Notebook 실행 시작
   - [ ] JDBC 연결 성공
   - [ ] SQL 실행 성공
   - [ ] Raw Bronze 테이블 생성 확인

2. Task 2: DLT Bronze/Silver
   - [ ] DLT Pipeline 시작
   - [ ] dlt-meta 라이브러리 설치 확인
   - [ ] Bronze 테이블 생성
   - [ ] Silver 테이블 생성

3. Task 3: Data Quality Validation
   - [ ] 검증 노트북 실행
   - [ ] 모든 검증 통과

---

## 📊 7. 결과 확인

### 테이블 확인

```sql
-- Raw Bronze
SELECT * FROM baikald1ws.sdp_poc.item_goods_option_rdb_raw LIMIT 10;

-- Bronze
SELECT * FROM baikald1ws.sdp_poc.item_goods_option_bronze LIMIT 10;

-- Silver
SELECT * FROM baikald1ws.sdp_poc.item_goods_option_silver LIMIT 10;
```

- [ ] Raw Bronze 데이터 확인
- [ ] Bronze 데이터 확인 (필터링 적용됨)
- [ ] Silver 데이터 확인 (CDC 적용됨)

### 메타데이터 확인

```sql
-- 실행 이력
SELECT * FROM baikald1ws.metadata.pipeline_execution_history
WHERE dataflow_id = 'item_goods_option'
ORDER BY start_time DESC;

-- 데이터 계보
SELECT * FROM baikald1ws.metadata.data_lineage
WHERE dataflow_id = 'item_goods_option';
```

- [ ] 실행 이력 기록 확인
- [ ] 데이터 계보 기록 확인

---

## 🔍 8. 모니터링

### Databricks UI에서 확인

- [ ] **Jobs** → `orchestrator_item_goods_option_full` 상태 확인
- [ ] **Pipelines** → `dlt_item_goods_option` 상태 확인
- [ ] **Data Explorer** → 테이블 생성 및 데이터 확인

### 로그 확인

```bash
# Job 로그
databricks jobs list-runs --job-id <job-id> --limit 1

# Pipeline 로그
databricks pipelines get <pipeline-id>
```

- [ ] Job 실행 로그 확인
- [ ] Pipeline 실행 로그 확인
- [ ] 에러 없음 확인

---

## 📈 9. 성능 확인

- [ ] RDB Ingestion 소요 시간 확인
- [ ] DLT Pipeline 소요 시간 확인
- [ ] 전체 파이프라인 소요 시간 확인
- [ ] 데이터 처리량 확인

**벤치마크:**
- RDB Ingestion: ~5-10분 (데이터량에 따라)
- DLT Pipeline: ~10-15분 (데이터량에 따라)
- Total: ~20-30분

---

## 🎯 10. 프로덕션 배포 (선택)

### 프로덕션 환경 준비

- [ ] 프로덕션 catalog 생성
- [ ] 프로덕션 secrets 설정
- [ ] 프로덕션 메타데이터 검증

### 프로덕션 배포

```bash
# 메타데이터 생성
./run_demo.sh prod

# 배포
databricks bundle deploy --target prod

# 스케줄 확인
# orchestrator_full_pipeline이 자동으로 실행되도록 설정됨 (매일 새벽 2시)
```

- [ ] 프로덕션 배포 완료
- [ ] 스케줄 활성화 확인
- [ ] 알림 설정 확인 (이메일)

---

## ✅ 최종 체크

- [ ] 모든 테이블 정상 생성
- [ ] 데이터 품질 검증 통과
- [ ] 메타데이터 추적 정상 작동
- [ ] 문서 작성 완료
- [ ] 팀 공유 완료

---

## 🆘 문제 발생 시

### 1. RDB 연결 실패
- JDBC URL 확인
- Secrets 확인
- 네트워크 연결 확인

### 2. DLT Pipeline 실패
- dlt-meta 라이브러리 설치 확인
- Dataflowspec 테이블 확인
- 권한 확인

### 3. 데이터 불일치
- SQL 쿼리 검증
- 필터 조건 확인
- CDC 설정 확인

---

**배포 성공을 기원합니다! 🚀**
