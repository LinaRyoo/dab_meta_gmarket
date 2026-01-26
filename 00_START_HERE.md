# 🚀 여기서 시작하세요!

통합 메타데이터 기반 DLT 파이프라인 - 하나의 YAML로 RDB부터 Silver까지 관리!

---

## ⚡ 5분 빠른 시작 (Mock 데이터) ⭐

```bash
# 1. 설정 생성
./run_demo.sh dev

# 2. 배포
databricks bundle deploy --target dev

# 3. 메타데이터 테이블 생성 (최초 1회)
databricks bundle run setup_metadata --target dev

# 4. 전체 파이프라인 테스트
databricks bundle run test_pipeline_with_mock_data --target dev
```

**5-7분 후 완료!** Mock 데이터 → DLT → Validation

### 결과 확인

```sql
SELECT COUNT(*) FROM baikald1ws.sdp_poc.item_goods_option_silver;
-- 예상: ~70 rows
```

---

## 📋 프로덕션 설정 (실제 RDB)

### 1. Secrets 설정

```bash
databricks secrets create-scope pipeline-cred-baikalx
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_username
databricks secrets put-secret --scope pipeline-cred-baikalx --key gdevdb02_password
```

### 2. 메타데이터 확인

`metadata/unified_pipeline_metadata.yml` 확인:
- JDBC URL
- Catalog/Schema 이름
- SQL 쿼리

### 3. 실행

```bash
./run_demo.sh dev
databricks bundle deploy --target dev
databricks bundle run orchestrator_full_pipeline --target dev
```

---

## 🎯 주요 특징

- ✅ **SSOT**: 하나의 YAML로 전체 관리
- ✅ **Mock 테스트**: RDB 없이 테스트
- ✅ **자동화**: 설정 자동 생성
- ✅ **Data Quality**: CDC + DQE 통합

---

## 📁 핵심 파일

```
metadata/unified_pipeline_metadata.yml  ← 여기만 수정!
scripts/unified_metadata_processor.py   → 자동 생성
databricks.yml                          → DABs 설정
```

---

## 🐛 문제 해결

### Secrets 에러
```bash
databricks secrets list --scope pipeline-cred-baikalx
```

### DLT 실패
- UI → Delta Live Tables → Logs 확인

### Instance Type 에러
- databricks.yml에서 cloud provider에 맞게 수정
- AWS: `i3.xlarge`
- Azure: `Standard_DS3_v2`
- GCP: `n1-standard-4`

---

## 📚 추가 문서

- [README.md](README.md) - 전체 가이드
- [CHECKLIST.md](CHECKLIST.md) - 배포 체크리스트
- [ARCHITECTURE.md](ARCHITECTURE.md) - 아키텍처

---

**✨ 이제 시작하세요!**
