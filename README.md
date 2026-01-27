# 🚀 dlt-meta 기반 통합 데이터 파이프라인

**Single Source of Truth (SSOT) 메타데이터 기반 RDB → Bronze → Silver 자동화 파이프라인**

[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-red)](https://databricks.com)
[![DLT](https://img.shields.io/badge/Delta%20Live%20Tables-Enabled-blue)](https://docs.databricks.com/delta-live-tables/)
[![Python](https://img.shields.io/badge/Python-3.8%2B-green)](https://python.org)
[![License](https://img.shields.io/badge/License-Internal-yellow)]()

---

## 📋 목차

- [개요](#-개요)
- [주요 기능](#-주요-기능)
- [아키텍처](#-아키텍처)
- [빠른 시작](#-빠른-시작)
- [프로젝트 구조](#-프로젝트-구조)
- [사용 가이드](#-사용-가이드)
- [모니터링](#-모니터링)
- [FAQ](#-faq)
- [문서](#-문서)
- [라이센스](#-라이센스)

---

## 🎯 개요

이 프로젝트는 **dlt-meta 프레임워크**를 기반으로 하나의 YAML 메타데이터 파일로 전체 데이터 파이프라인을 관리하는 통합 솔루션입니다.

### 핵심 가치

- **📄 단일 메타데이터 소스**: 하나의 YAML로 RDB 연동, Bronze, Silver 레이어를 모두 정의
- **🤖 자동화**: 메타데이터 변경 시 모든 설정 파일 자동 생성
- **📊 데이터 품질**: DLT Expectations를 통한 자동 품질 검증
- **🔍 모니터링**: 실시간 파이프라인 상태 모니터링 및 자동 알림
- **🔄 CDC 지원**: Change Data Capture를 통한 증분 처리
- **🎯 확장성**: 새로운 테이블 추가가 메타데이터만으로 가능

---

## ✨ 주요 기능

### 1. 통합 메타데이터 관리
```yaml
# unified_pipeline_metadata.yml
dataflows:
  - dataflow_id: item_goods_option
    rdb_connection: {...}
    bronze_config: {...}
    silver_config: {...}
    dqe_rules: {...}
```
→ 자동 생성:
- RDB Ingestion 설정
- Bronze DLT 설정
- Silver DLT 설정
- DABs Job 정의
- 메타데이터 테이블 DDL

### 2. 데이터 품질 관리
- ✅ **Expectations**: `expect` (Warning) / `expect_or_fail` (Critical)
- ✅ **Quarantine Table**: 품질 위반 데이터 자동 분리
- ✅ **Event Log**: 품질 메트릭 자동 기록

### 3. 자동 모니터링
- 🔍 **파이프라인 실패 감지**: 최근 N시간 실패 체크
- 📊 **데이터 품질 이상치**: Z-Score 기반 통계적 탐지
- ⏱️ **SLA 초과 감지**: Stage별 실행 시간 모니터링
- 📈 **성공률 추적**: 24시간 성공률 추적
- 🔔 **다중 알림**: Slack, Email 통합

### 4. CDC 및 증분 처리
- **SCD Type 1**: 최신 상태 유지
- **Delete 처리**: Soft delete 지원
- **순서 보장**: `sequence_by` 컬럼 기반

### 5. Databricks Asset Bundles (DABs)
- 🚀 **원클릭 배포**: `databricks bundle deploy`
- 🔄 **환경 관리**: dev, staging, prod
- 📅 **스케줄링**: Cron 기반 자동 실행
- 🔐 **Secrets 통합**: Databricks Secrets 사용

---

## 🏗️ 아키텍처

```
┌──────────────────────────────────────────────────────────────┐
│             Unified Metadata (YAML)                          │
│        unified_pipeline_metadata.yml                         │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│       unified_metadata_processor.py                          │
│  ├── rdb_ingestion_*.json                                    │
│  ├── onboard_dataflow_*.json                                 │
│  ├── silver_transformations_*.json                           │
│  ├── dabs_*.yml                                              │
│  ├── metadata_ddl_*.sql                                      │
│  └── dqe/*.json                                              │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│          Databricks Asset Bundles (DABs)                     │
│  Jobs:                                                       │
│  ├── setup_metadata                                          │
│  ├── create_mock_data                                        │
│  ├── orchestrator_full_pipeline                             │
│  ├── dlt_item_goods_option                                  │
│  └── pipeline_monitoring                                     │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│                    Data Flow                                 │
│                                                              │
│  RDB ──▶ Bronze ──▶ Silver ──▶ Gold                         │
│  (MariaDB) (Delta)  (Delta)    (Future)                     │
│              │         │                                     │
│              ▼         ▼                                     │
│          Raw CDC   Quarantine                                │
└──────────────────────────────────────────────────────────────┘
```

상세 아키텍처는 [ARCHITECTURE.md](./docs/ARCHITECTURE.md) 참조

---

## ⚡ 빠른 시작

### 사전 요구사항
- Databricks Workspace
- Unity Catalog
- Databricks CLI
- Python 3.8+

### 5분 빠른 시작

```bash
# 1. 메타데이터 처리
python3 scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev

# 2. 배포
databricks bundle deploy --target dev

# 3. 메타데이터 초기화
databricks bundle run setup_metadata --target dev

# 4. Mock 데이터로 테스트
databricks bundle run create_mock_data_with_bad_records --target dev

# 5. DLT 파이프라인 실행
databricks bundle run dlt_item_goods_option --target dev
```

상세 가이드는 [START.md](./docs/START.md) 참조

---

## 📁 프로젝트 구조

```
dab_meta_gmarket/
├── metadata/
│   └── unified_pipeline_metadata.yml       # 통합 메타데이터 (SSOT)
├── scripts/
│   ├── unified_metadata_processor.py       # 메타데이터 처리 엔진
│   └── create_mock_data.py                 # Mock 데이터 생성
├── notebooks/
│   ├── rdb_ingestion_runner.py             # RDB → Delta
│   ├── data_quality_validator.py           # 품질 검증
│   └── pipeline_monitor.py                 # 자동 모니터링
├── generated/                               # 자동 생성 파일
│   ├── rdb_ingestion_*.json
│   ├── onboard_dataflow_*.json
│   ├── silver_transformations_*.json
│   ├── dabs_*.yml
│   ├── metadata_ddl_*.sql
│   └── dqe/
│       └── silver_*_expectations.json
├── databricks.yml                           # DABs 메인 설정
├── docs/
│   ├── START.md                             # 빠른 시작 가이드
│   ├── ARCHITECTURE.md                      # 아키텍처 설명
│   ├── CHECKLIST.md                         # 프로덕션 체크리스트
│   ├── MONITORING_SETUP.md                  # 모니터링 설정
│   ├── MONITORING_ARCHITECTURE.md           # 모니터링 아키텍처
│   ├── MONITORING_QUICK_START.md            # 모니터링 빠른 시작
│   └── TROUBLESHOOTING.md                   # 트러블슈팅
└── README.md                                # 이 파일
```

---

## 📖 사용 가이드

### 1. 새로운 테이블 추가

#### Step 1: 메타데이터 정의
```yaml
# metadata/unified_pipeline_metadata.yml
dataflows:
  - dataflow_id: new_table_name
    source_table: "schema.table_name"
    rdb_connection:
      host: "{{secrets/scope/host}}"
      # ...
    bronze_config:
      keys: ["PRIMARY_KEY"]
      sequence_by: "UPDATE_TIME"
    silver_config:
      transformations:
        - "UPPER(column_name) as column_name_upper"
      dqe_rules:
        expect:
          valid_column: "column_name IS NOT NULL"
```

#### Step 2: 설정 파일 생성
```bash
python3 scripts/unified_metadata_processor.py \
  --metadata-file metadata/unified_pipeline_metadata.yml \
  --output-dir generated/ \
  --environment dev
```

#### Step 3: 배포 및 실행
```bash
databricks bundle deploy --target dev
databricks bundle run dlt_new_table_name --target dev
```

### 2. 데이터 품질 규칙 추가

```yaml
silver_config:
  dqe_rules:
    expect:
      # Warning만, 데이터는 통과
      valid_price: "PRICE >= 0"
      valid_date: "ORDER_DATE IS NOT NULL"
    expect_or_fail:
      # Critical, 데이터 reject
      valid_id: "ID IS NOT NULL"
      valid_email: "EMAIL LIKE '%@%'"
```

### 3. 모니터링 설정

```bash
# 모니터링 Job 실행 (매시간 자동)
databricks bundle run pipeline_monitoring --target dev

# Slack 알림 활성화
databricks secrets put-secret \
  pipeline-cred-baikalx slack_webhook_url \
  --string-value "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### 4. Mock 데이터 생성

```bash
# 정상 데이터만
databricks bundle run create_mock_data --target dev

# Bad Data 추가 (Expectation 테스트)
databricks bundle run append_bad_data --target dev

# 한번에 전체 (정상 + Bad)
databricks bundle run create_mock_data_with_bad_records --target dev
```

---

## 🔍 모니터링

### 자동 모니터링 항목

| 항목 | 설명 | 임계값 | 액션 |
|------|------|--------|------|
| **파이프라인 실패** | status = 'failed' | 1건 이상 | 🔴 Critical 알림 |
| **데이터 이상치** | Z-Score > 3 | 3σ 초과 | ⚠️ Warning 알림 |
| **SLA 초과** | duration > limit | Stage별 설정 | ⚠️ Warning 알림 |
| **성공률 저하** | 24h success rate | < 95% | ⚠️ Warning 알림 |
| **데이터 신선도** | 마지막 성공 시간 | > 2시간 | ⚠️ Warning 알림 |

### 모니터링 대시보드

```sql
-- 실행 현황
SELECT 
    dataflow_id,
    stage,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
    AVG(duration_seconds) as avg_duration,
    MAX(start_time) as last_run
FROM baikald1ws.metadata.pipeline_execution_history
WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY dataflow_id, stage
ORDER BY last_run DESC;
```

상세 내용은 [MONITORING_SETUP.md](./docs/MONITORING_SETUP.md) 참조

---

## ❓ FAQ

### Q1: 새로운 테이블을 추가하려면?
**A**: `unified_pipeline_metadata.yml`에 dataflow를 추가하고 메타데이터 프로세서를 실행하세요.

### Q2: RDB 연결 정보를 어떻게 설정하나요?
**A**: Databricks Secrets에 저장하고 YAML에서 `{{secrets/scope/key}}` 형식으로 참조하세요.

### Q3: Quarantine 테이블의 데이터는 어떻게 처리하나요?
**A**: 
1. 정기적으로 검토 (`SELECT * FROM ...quarantine`)
2. 데이터 수정 후 재처리
3. 또는 비즈니스 로직에 따라 처리

### Q4: Mock 데이터 없이 실제 RDB로 테스트하려면?
**A**: `orchestrator_full_pipeline` Job을 실행하세요:
```bash
databricks bundle run orchestrator_item_goods_option_full --target dev
```

### Q5: 모니터링 알림이 오지 않아요.
**A**: 
1. Slack Webhook URL이 Secrets에 등록되어 있는지 확인
2. `pipeline_monitoring` Job의 파라미터 확인
3. 테스트 실패 데이터 삽입 후 재실행

### Q6: 파이프라인이 실패했어요.
**A**: Databricks UI에서 에러 로그를 확인하세요. 일반적인 원인:
- RDB 연결 실패 (Secrets 확인)
- 테이블이 존재하지 않음 (setup_metadata 실행)
- 리소스 부족 (Cluster 크기 조정)

### Q7: 프로덕션 배포 전 체크할 것은?
**A**: [CHECKLIST.md](./docs/CHECKLIST.md)를 따라하세요.

### Q8: 데이터 품질 규칙을 언제 사용하나요?
**A**:
- **expect**: 비정상이지만 분석 가능한 데이터 (예: 선택 필드 누락)
- **expect_or_fail**: 필수 필드 누락 등 치명적 품질 이슈

### Q9: 여러 환경(dev/prod)을 어떻게 관리하나요?
**A**: 
```bash
# Dev
databricks bundle deploy --target dev
# Prod
databricks bundle deploy --target prod
```
`databricks.yml`에서 환경별 설정 분리

### Q10: 비용 최적화 팁은?
**A**:
- Auto-termination 활성화 (30분)
- DLT Enhanced Autoscaling 사용
- Cluster 크기 최적화
- Retention 정책 설정 (7일)

---

## 📚 문서

### 시작하기
- **[START.md](./docs/START.md)** - 5분 빠른 시작 가이드
- **[ARCHITECTURE.md](./docs/ARCHITECTURE.md)** - 아키텍처 상세 설명

### 운영
- **[CHECKLIST.md](./docs/CHECKLIST.md)** - 프로덕션 배포 체크리스트


---

## 🤝 기여

이 프로젝트는 내부 프로젝트입니다. 개선 사항이나 버그 리포트는:
- **Jira**: [PROJECT-XXX]
- **Slack**: #data-platform-dev
- **Email**: data-engineering@company.com

---

## 📄 라이센스

Internal Use Only - Company Confidential

---

## 👥 팀

**Data Platform Engineering Team**
- Lead: [Name] - [email]
- Developer: [Name] - [email]
- DevOps: [Name] - [email]

---

## 🎯 로드맵

### Q1 2026
- [x] dlt-meta 통합
- [x] 통합 메타데이터 관리
- [x] 자동 모니터링
- [x] DABs 기반 배포

### Q2 2026
- [ ] Gold Layer 추가
- [ ] Streaming 지원
- [ ] Multi-region 배포
- [ ] Advanced Analytics 연동

### Q3 2026
- [ ] ML Feature Store 통합
- [ ] Real-time 모니터링 대시보드
- [ ] Auto-remediation

---

## 📞 지원

**문제가 발생하면:**
1. Databricks UI에서 Job 로그 확인
2. Slack #data-platform-support에 문의
3. On-call: [phone] (긴급 시)

---

## 🙏 감사

- **dlt-meta**: Databricks Labs의 훌륭한 프레임워크
- **Databricks**: 강력한 플랫폼 제공
- **Team Members**: 모든 기여자들께 감사드립니다

---

**Built with ❤️ by Data Platform Engineering Team**

*Last Updated: 2026-01-27*
