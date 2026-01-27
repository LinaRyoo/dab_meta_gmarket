# ✅ Production Deployment Checklist

**dlt-meta 기반 통합 데이터 파이프라인 - 프로덕션 배포 체크리스트**

프로덕션 환경에 배포하기 전에 이 체크리스트를 확인하세요.

---

## 🔐 1. 보안 (Security)

### Secrets 관리
- [ ] Databricks Secrets Scope 생성 완료
  ```bash
  databricks secrets create-scope pipeline-cred-baikalx
  ```
- [ ] RDB 연결 정보 Secrets 등록
  - [ ] `db_host`
  - [ ] `db_port`
  - [ ] `db_username`
  - [ ] `db_password`
  - [ ] `db_database`
- [ ] Slack Webhook URL 등록 (선택)
  - [ ] `slack_webhook_url`
- [ ] Email SMTP 설정 (선택)

### Unity Catalog 권한
- [ ] Production Catalog 생성
  ```sql
  CREATE CATALOG IF NOT EXISTS baikald1ws;
  ```
- [ ] Service Principal 생성 및 권한 부여
  ```sql
  GRANT ALL PRIVILEGES ON CATALOG baikald1ws TO `dlt-meta-prod-sp`;
  ```
- [ ] 팀별 접근 권한 설정
  - [ ] Data Engineering: ALL PRIVILEGES
  - [ ] Analytics: SELECT
  - [ ] BI Team: SELECT (specific schemas only)

### 네트워크 보안
- [ ] RDB 방화벽 규칙 설정 (Databricks IP 허용)
- [ ] VPC 피어링 설정 (필요 시)
- [ ] Private Link 설정 (필요 시)

---

## 🎯 2. 메타데이터 설정 (Metadata Configuration)

### YAML 검증
- [ ] `unified_pipeline_metadata.yml` 필수 필드 확인
  - [ ] `rdb_connection` (host, port, database)
  - [ ] `dataflows` 정의
  - [ ] `bronze_config` (keys, sequence_by)
  - [ ] `silver_config` (transformations, dqe)
- [ ] 환경별 설정 분리 (dev/prod)
  ```yaml
  environments:
    dev:
      catalog: "baikald1ws_dev"
    prod:
      catalog: "baikald1ws"
  ```
- [ ] Git으로 버전 관리
  - [ ] `.gitignore`에 secrets 추가
  - [ ] 변경 이력 기록

### 데이터 품질 규칙
- [ ] DQE 정의 검토
  - [ ] `expect` vs `expect_or_fail` 구분 명확
  - [ ] Critical 데이터는 `expect_or_fail` 사용
  - [ ] Warning 데이터는 `expect` 사용
- [ ] Quarantine 테이블 처리 프로세스 정의
  - [ ] 검토 주기 (일일/주간)
  - [ ] 재처리 절차
  - [ ] 알림 담당자

---

## 🚀 3. Databricks 리소스 (Databricks Resources)

### Compute
- [ ] Production 클러스터 정책 설정
  - [ ] Auto-termination: 30분
  - [ ] Autoscaling: 활성화
  - [ ] Instance type: 워크로드에 맞게 선택
- [ ] SQL Warehouse 생성 및 ID 확인
  - [ ] `databricks.yml`의 `warehouse_id` 업데이트
- [ ] DLT 파이프라인 설정
  - [ ] Enhanced Autoscaling: 활성화
  - [ ] Channel: `CURRENT` (Stable) or `PREVIEW`
  - [ ] Cluster mode: `ENHANCED` (권장)

### Storage
- [ ] Unity Catalog 스토리지 위치 확인
- [ ] Delta Lake 최적화 설정
  - [ ] Auto Optimize: 활성화
  - [ ] Auto Compaction: 활성화
- [ ] Retention 정책 설정
  ```sql
  ALTER TABLE ... SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
  );
  ```

### DABs 설정
- [ ] `databricks.yml` Production 설정 검토
  ```yaml
  targets:
    prod:
      mode: production
      run_as:
        service_principal_name: "dlt-meta-prod-sp"
  ```
- [ ] Job 스케줄 설정
  - [ ] 파이프라인 실행: Cron 표현식
  - [ ] 모니터링: 매시간
- [ ] 알림 설정
  - [ ] Job 실패 시 Email/Slack
  - [ ] SLA 위반 시 알림

---

## 🔍 4. 테스트 (Testing)

### 개발 환경 테스트
- [ ] Mock 데이터로 End-to-End 테스트
  ```bash
  databricks bundle run create_mock_data_with_bad_records --target dev
  databricks bundle run dlt_item_goods_option --target dev
  ```
- [ ] 데이터 품질 검증
  - [ ] Silver 테이블 레코드 수 확인
  - [ ] Quarantine 테이블 확인
  - [ ] Expectation 작동 확인
- [ ] 모니터링 테스트
  - [ ] 실패 시뮬레이션
  - [ ] 알림 수신 확인 (Slack/Email)

### 통합 테스트
- [ ] RDB 연결 테스트
  ```python
  # Test connection
  df = spark.read.jdbc(url, table, properties)
  df.count()
  ```
- [ ] CDC 동작 확인
  - [ ] INSERT 처리
  - [ ] UPDATE 처리
  - [ ] DELETE 처리
- [ ] 증분 처리 테스트
  - [ ] 새 데이터 추가 후 재실행
  - [ ] 중복 방지 확인

### 성능 테스트
- [ ] 대량 데이터 처리 테스트 (최소 7일치)
- [ ] 실행 시간 측정 및 SLA 검증
  - [ ] RDB Ingestion: < 15분
  - [ ] Bronze: < 10분
  - [ ] Silver: < 15분
- [ ] 리소스 사용량 모니터링
  - [ ] CPU, Memory, Disk I/O
  - [ ] Cluster 크기 조정

---

## 📊 5. 모니터링 및 알림 (Monitoring & Alerting)

### 메타데이터 테이블 확인
- [ ] 메타데이터 테이블 생성 완료
  ```bash
  databricks bundle run setup_metadata --target prod
  ```
- [ ] 테이블 존재 확인
  - [ ] `metadata.pipeline_execution_history`
  - [ ] `metadata.data_lineage`
  - [ ] `metadata.monitoring_history`
  - [ ] `metadata.metadata_versions`

### 모니터링 Job 설정
- [ ] `pipeline_monitoring` Job 배포
  ```bash
  databricks bundle deploy --target prod
  ```
- [ ] 스케줄 확인 (매시간 실행)
- [ ] 첫 실행 및 결과 확인
  ```bash
  databricks bundle run pipeline_monitoring --target prod
  ```
- [ ] 알림 채널 설정
  - [ ] Slack Webhook 연동
  - [ ] Email 수신자 목록

### 대시보드 설정 (선택)
- [ ] Databricks SQL 대시보드 생성
  - [ ] 파이프라인 실행 현황
  - [ ] 데이터 품질 트렌드
  - [ ] SLA 달성률
- [ ] Grafana/Datadog 연동 (선택)

---

## 🔄 6. 운영 프로세스 (Operations)

### 일일 점검
- [ ] 파이프라인 실행 상태 확인
- [ ] Quarantine 테이블 검토
- [ ] 알림 확인 및 조치

### 주간 점검
- [ ] 성능 메트릭 리뷰
  - [ ] 평균 실행 시간
  - [ ] 처리 레코드 수
  - [ ] 에러율
- [ ] 데이터 품질 리포트
- [ ] Cluster 리소스 사용률 분석

### 월간 점검
- [ ] SLA 달성률 리뷰
- [ ] 비용 최적화 검토
- [ ] 메타데이터 버전 업데이트 검토

### 장애 대응
- [ ] Runbook 작성
  - [ ] 파이프라인 실패 시 대응
  - [ ] RDB 연결 실패 시 대응
  - [ ] Cluster 리소스 부족 시 대응
- [ ] On-call 담당자 지정
- [ ] Escalation 프로세스 정의

---

## 📝 7. 문서화 (Documentation)

### 필수 문서
- [ ] README.md (프로젝트 개요)
- [ ] START.md (빠른 시작 가이드)
- [ ] ARCHITECTURE.md (아키텍처 설명)
- [ ] 이 CHECKLIST.md

### 운영 문서
- [ ] Runbook (장애 대응)
- [ ] 데이터 사전 (Data Dictionary)
  - [ ] 테이블 스키마
  - [ ] 컬럼 설명
  - [ ] 비즈니스 로직
- [ ] 변경 이력 (CHANGELOG.md)

### 팀 공유
- [ ] Wiki/Confluence 페이지 작성
- [ ] 팀 교육 실시
- [ ] 담당자 연락처 공유

---

## 🌐 8. 배포 (Deployment)

### Pre-Production 검증
- [ ] Staging 환경 배포 완료
- [ ] Smoke Test 통과
- [ ] Stakeholder 승인

### Production 배포
- [ ] 배포 시간 공지 (유지보수 창)
- [ ] 메타데이터 처리
  ```bash
  python3 scripts/unified_metadata_processor.py \
    --metadata-file metadata/unified_pipeline_metadata.yml \
    --output-dir generated/ \
    --environment prod
  ```
- [ ] DABs 배포
  ```bash
  databricks bundle deploy --target prod
  ```
- [ ] 메타데이터 테이블 초기화
  ```bash
  databricks bundle run setup_metadata --target prod
  ```
- [ ] 첫 파이프라인 실행
  ```bash
  databricks bundle run orchestrator_item_goods_option_full --target prod
  ```

### Post-Deployment 검증
- [ ] 파이프라인 정상 실행 확인
- [ ] 데이터 품질 확인
- [ ] 모니터링 정상 작동 확인
- [ ] 알림 수신 확인
- [ ] 팀 공지

---

## 🔄 9. 롤백 계획 (Rollback Plan)

### 롤백 준비
- [ ] 이전 버전 백업
  - [ ] `databricks.yml`
  - [ ] `unified_pipeline_metadata.yml`
  - [ ] Generated files
- [ ] 롤백 절차 문서화

### 롤백 실행 (필요 시)
- [ ] 이전 버전으로 복원
- [ ] DABs 재배포
- [ ] 데이터 정합성 확인
- [ ] 팀 공지

---

## 📈 10. 지속적 개선 (Continuous Improvement)

### 정기 검토
- [ ] 월간 성능 리뷰 회의
- [ ] 분기별 아키텍처 리뷰
- [ ] 연간 비용 최적화

### 피드백 수집
- [ ] 사용자 피드백 (Analytics 팀)
- [ ] 운영팀 피드백
- [ ] 개선 사항 백로그 관리

### 기술 부채 관리
- [ ] 코드 리팩토링 계획
- [ ] 레거시 제거
- [ ] 최신 Databricks 기능 적용

---

## ✅ 최종 확인

### 배포 전 최종 체크
- [ ] 모든 체크리스트 항목 완료
- [ ] Stakeholder 승인
- [ ] 배포 시간 확정
- [ ] 롤백 계획 준비
- [ ] On-call 담당자 준비

### 배포 완료 후
- [ ] 배포 완료 공지
- [ ] 첫 24시간 모니터링 강화
- [ ] 이슈 트래킹
- [ ] 회고 (Retrospective)

---

## 🆘 문제 발생 시

**즉시 조치**:
1. 파이프라인 일시 중지
2. 로그 확인 및 수집
3. On-call 담당자에게 연락
4. Runbook 참조하여 대응

**관련 문서**:
- [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)
- [MONITORING_SETUP.md](./MONITORING_SETUP.md)

---

## 📞 연락처

- **Data Engineering Lead**: [email]
- **On-call (Weekday)**: [email]
- **On-call (Weekend)**: [email]
- **Slack Channel**: #data-platform-support

---

**이 체크리스트를 따라하면 안정적인 프로덕션 배포를 보장할 수 있습니다!** ✅
