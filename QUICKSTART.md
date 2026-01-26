# 🚀 통합 메타데이터 SSOT - 빠른 시작 가이드

## 핵심 개념

**하나의 메타데이터 파일**로 RDB ingestion부터 dlt-meta Bronze/Silver까지 전체 관리!

```
unified_pipeline_metadata.yml (SSOT)
    ↓
[자동 생성]
    ↓
RDB Ingestion + dlt-meta Onboarding + DABs + Metadata Tables
```

---

## 1분 만에 시작하기

### 1️⃣ 메타데이터 파일 준비

`metadata/unified_pipeline_metadata.yml`:

```yaml
dataflows:
  - dataflow_id: "item_goods_option"
    dataflow_group: "item_group"
    
    source:
      type: "rdb_jdbc"
      connection:
        connection_name: "gdevdb02"
        jdbc_url: "jdbc:sqlserver://your-server:1433;database=item"
      extraction:
        prepare_query: |
          SELECT * INTO #temp FROM source_table WHERE date >= '2025-01-01'
        main_query: |
          SELECT * FROM #temp
```

### 2️⃣ 설정 자동 생성

```bash
# 실행 권한 부여
chmod +x run_demo.sh

# 데모 실행
./run_demo.sh dev
```

**생성되는 파일들:**
- ✅ `generated/rdb_ingestion_item_goods_option.json` - RDB 추출 설정
- ✅ `generated/onboarding_item_goods_option.json` - dlt-meta 설정
- ✅ `generated/transformations_item_goods_option.json` - Silver 변환 로직
- ✅ `generated/dabs_item_goods_option.yml` - DABs 리소스
- ✅ `generated/metadata_ddl_item_goods_option.sql` - 메타데이터 테이블

### 3️⃣ Databricks 배포

```bash
# Databricks CLI 인증
databricks auth login --host https://your-workspace.cloud.databricks.com

# Secrets 설정
databricks secrets create-scope db_credentials
databricks secrets put-secret --scope db_credentials --key gdevdb02_username
databricks secrets put-secret --scope db_credentials --key gdevdb02_password

# 배포
databricks bundle deploy --target dev

# 실행
databricks bundle run orchestrator_item_goods_option_full --target dev
```

---

## 📊 메타데이터 확인

### 실행 이력 조회

```sql
SELECT 
  execution_id,
  dataflow_id,
  stage,
  status,
  rows_processed,
  duration_seconds,
  start_time
FROM baikald1ws.metadata.pipeline_execution_history
WHERE dataflow_id = 'item_goods_option'
ORDER BY start_time DESC;
```

### 데이터 계보 확인

```sql
SELECT 
  source_table,
  target_table,
  transformation_logic
FROM baikald1ws.metadata.data_lineage
WHERE dataflow_id = 'item_goods_option';
```

---

## 🎯 주요 장점

### ✅ SSOT (Single Source of Truth)
- 하나의 YAML로 모든 메타데이터 관리
- 중복 제거 및 일관성 보장

### ✅ 자동화
- 수동 설정 작성 불필요
- 실수 방지

### ✅ 버전 관리
- Git으로 메타데이터 변경 이력 추적
- 롤백 가능

### ✅ 환경 분리
- dev/prod 환경별 다른 설정
- 안전한 배포

### ✅ 확장성
- 새 테이블 추가 = YAML만 수정
- 일관된 패턴 유지

---

## 🔄 워크플로우 비교

### ❌ 기존 방식 (Legacy)

```
1. MongoDB에 bronze 메타 작성
2. MongoDB에 silver 메타 작성
3. RDB ingestion 스크립트 작성
4. dlt-meta 설정 확인
5. 각각 별도 실행
```

**문제점:**
- 메타데이터 중복 관리
- 불일치 발생 가능
- 추적 어려움

### ✅ 새로운 방식 (통합 메타데이터)

```
1. unified_pipeline_metadata.yml 작성
2. python unified_metadata_processor.py 실행
3. databricks bundle deploy
```

**장점:**
- ✨ SSOT로 일관성 보장
- ✨ 자동 생성으로 실수 방지
- ✨ Git 버전 관리
- ✨ 환경별 배포 자동화

---

## 📈 마이그레이션 가이드

### Legacy → 통합 메타데이터 변환

**Before (Legacy MongoDB):**

```javascript
// Bronze meta
{
  "pipeline_name": "sdppoc_gdevdb02_item_goods_option_bronze",
  "stages": [
    {
      "source": {
        "source_type": "rdb",
        "query": "SELECT * FROM ..."
      }
    }
  ]
}

// Silver meta (별도 문서)
{
  "pipeline_name": "sdppoc_gdevdb02_item_goods_option_silver",
  "stages": [...]
}
```

**After (통합 YAML):**

```yaml
dataflows:
  - dataflow_id: "item_goods_option"
    source:
      type: "rdb_jdbc"
      extraction:
        main_query: "SELECT * FROM ..."
    bronze:
      # ... 설정
    silver:
      # ... 설정
```

---

## 🛠️ 실전 예시

### 예시 1: 새 테이블 추가

```yaml
# metadata/unified_pipeline_metadata.yml
dataflows:
  - dataflow_id: "product_inventory"  # 새로 추가!
    dataflow_group: "item_group"
    source:
      type: "rdb_jdbc"
      # ...
```

```bash
# 재생성 및 재배포
./run_demo.sh dev
databricks bundle deploy --target dev
```

### 예시 2: 변환 로직 수정

```yaml
# unified_pipeline_metadata.yml
silver:
  transformations:
    select_expressions:
      - "OPT_NO"
      - "UPPER(OPT_NM) as OPT_NM"  # 로직 수정
```

```bash
# 재생성 및 재배포
./run_demo.sh dev
databricks bundle deploy --target dev
```

### 예시 3: 스케줄 변경

```yaml
schedule:
  cron: "0 3 * * *"  # 새벽 2시 → 3시
  timezone: "Asia/Seoul"
```

---

## 💡 팁 & 트릭

### 1. 메타데이터 검증

```bash
# YAML 문법 검증
python -c "import yaml; yaml.safe_load(open('metadata/unified_pipeline_metadata.yml'))"
```

### 2. 로컬 테스트

```bash
# 설정만 생성 (배포 안함)
python scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev
```

### 3. Git 워크플로우

```bash
# 메타데이터 변경
vim metadata/unified_pipeline_metadata.yml

# 설정 재생성
./run_demo.sh dev

# Git 커밋 (메타데이터만)
git add metadata/
git commit -m "Update item_goods_option metadata"

# generated/ 는 .gitignore에 추가 권장
echo "generated/" >> .gitignore
```

---

## 🆘 자주 묻는 질문

### Q: 기존 MongoDB 메타는 어떻게 하나요?
**A:** 점진적으로 마이그레이션하세요. 새로운 테이블부터 통합 메타데이터를 사용하고, 기존 테이블은 필요 시 변환하세요.

### Q: RDB가 아닌 다른 소스는?
**A:** 통합 메타데이터에서 `source.type: "delta"` 또는 `"cloudFiles"` 사용 가능. RDB 단계를 건너뛰고 dlt-meta부터 시작하면 됩니다.

### Q: 운영 환경 배포 시 주의사항은?
**A:** `--target prod`로 배포 전 dev 환경에서 충분히 테스트하세요. 메타데이터 변경 이력을 Git으로 관리하세요.

---

## 📚 더 알아보기

- [전체 가이드](README_UNIFIED_METADATA.md)
- [dlt-meta 문서](https://databrickslabs.github.io/dlt-meta/)
- [DABs 가이드](https://docs.databricks.com/dev-tools/bundles/)

---

**🎉 이제 SSOT로 메타데이터를 관리하세요!**
