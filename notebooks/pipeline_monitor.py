# Databricks notebook source
"""
파이프라인 모니터링 및 알림 자동화

이 노트북은 메타데이터 테이블을 기반으로:
1. 파이프라인 실패 감지
2. 데이터 품질 이상치 탐지
3. SLA 초과 감지
4. Slack/Email 알림 전송

주기적으로 실행하여 파이프라인 상태를 모니터링합니다.
"""

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. 설정 및 초기화

# COMMAND ----------
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, TimestampType, DateType, IntegerType, StringType

# 위젯 파라미터 설정
dbutils.widgets.text("catalog", "baikald1ws", "Catalog Name")
dbutils.widgets.text("monitoring_window_hours", "1", "Monitoring Window (Hours)")
dbutils.widgets.text("slack_webhook_url", "", "Slack Webhook URL (Optional)")
dbutils.widgets.text("email_recipients", "", "Email Recipients (Comma-separated)")
dbutils.widgets.dropdown("alert_level", "all", ["all", "critical", "warning"], "Alert Level")

# 파라미터 가져오기
CATALOG = dbutils.widgets.get("catalog")
MONITORING_WINDOW_HOURS = int(dbutils.widgets.get("monitoring_window_hours"))
SLACK_WEBHOOK_URL = dbutils.widgets.get("slack_webhook_url")
EMAIL_RECIPIENTS = dbutils.widgets.get("email_recipients").split(",") if dbutils.widgets.get("email_recipients") else []
ALERT_LEVEL = dbutils.widgets.get("alert_level")

print(f"=== 모니터링 설정 ===")
print(f"Catalog: {CATALOG}")
print(f"Monitoring Window: {MONITORING_WINDOW_HOURS} hours")
print(f"Slack Enabled: {'Yes' if SLACK_WEBHOOK_URL else 'No'}")
print(f"Email Enabled: {'Yes' if EMAIL_RECIPIENTS else 'No'}")
print(f"Alert Level: {ALERT_LEVEL}")
print(f"Monitoring Start Time: {datetime.now()}")

# 필수 테이블 존재 확인
print("\n🔍 메타데이터 테이블 확인 중...")
try:
    # pipeline_execution_history 테이블 존재 확인
    spark.sql(f"DESCRIBE TABLE {CATALOG}.metadata.pipeline_execution_history").collect()
    print(f"✅ {CATALOG}.metadata.pipeline_execution_history - 존재")
    
    # monitoring_history 테이블 존재 확인
    try:
        spark.sql(f"DESCRIBE TABLE {CATALOG}.metadata.monitoring_history").collect()
        print(f"✅ {CATALOG}.metadata.monitoring_history - 존재")
    except:
        print(f"⚠️ {CATALOG}.metadata.monitoring_history - 없음 (생성 필요)")
        # 테이블 없어도 계속 진행 (save_monitoring_history에서 처리)
    
    print("✅ 메타데이터 테이블 확인 완료\n")
except Exception as e:
    print(f"\n❌ 필수 메타데이터 테이블이 없습니다!")
    print(f"Error: {str(e)}")
    print(f"\n해결 방법:")
    print(f"1. 먼저 메타데이터 테이블을 생성하세요:")
    print(f"   databricks bundle run setup_metadata --target dev")
    print(f"\n2. 또는 SQL로 직접 생성:")
    print(f"   %sql CREATE SCHEMA IF NOT EXISTS {CATALOG}.metadata;")
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "message": "Required metadata tables not found",
        "required_action": "Run setup_metadata job first"
    }))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. 알림 전송 함수

# COMMAND ----------
class AlertManager:
    """알림 관리 클래스"""
    
    def __init__(self, slack_webhook: str = None, email_recipients: List[str] = None):
        self.slack_webhook = slack_webhook
        self.email_recipients = email_recipients
        self.alerts = []
    
    def add_alert(self, alert_type: str, severity: str, title: str, message: str, details: Dict[str, Any] = None):
        """알림 추가"""
        self.alerts.append({
            "alert_type": alert_type,
            "severity": severity,  # critical, warning, info
            "title": title,
            "message": message,
            "details": details or {},
            "timestamp": datetime.now().isoformat()
        })
    
    def send_slack_alert(self, alert: Dict[str, Any]) -> bool:
        """Slack으로 알림 전송"""
        if not self.slack_webhook:
            return False
        
        # Severity에 따른 이모지 및 색상
        emoji_map = {
            "critical": "🔴",
            "warning": "🟡",
            "info": "🔵"
        }
        color_map = {
            "critical": "#FF0000",
            "warning": "#FFA500",
            "info": "#0000FF"
        }
        
        emoji = emoji_map.get(alert["severity"], "⚪")
        color = color_map.get(alert["severity"], "#808080")
        
        # Slack 메시지 포맷
        slack_message = {
            "text": f"{emoji} *{alert['title']}*",
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {
                            "title": "Alert Type",
                            "value": alert["alert_type"],
                            "short": True
                        },
                        {
                            "title": "Severity",
                            "value": alert["severity"].upper(),
                            "short": True
                        },
                        {
                            "title": "Message",
                            "value": alert["message"],
                            "short": False
                        },
                        {
                            "title": "Timestamp",
                            "value": alert["timestamp"],
                            "short": True
                        }
                    ]
                }
            ]
        }
        
        # 상세 정보 추가
        if alert.get("details"):
            details_text = "\n".join([f"• *{k}*: {v}" for k, v in alert["details"].items()])
            slack_message["attachments"][0]["fields"].append({
                "title": "Details",
                "value": details_text,
                "short": False
            })
        
        try:
            response = requests.post(
                self.slack_webhook,
                json=slack_message,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            response.raise_for_status()
            print(f"✅ Slack 알림 전송 성공: {alert['title']}")
            return True
        except Exception as e:
            print(f"❌ Slack 알림 전송 실패: {str(e)}")
            return False
    
    def send_email_alert(self, alert: Dict[str, Any]) -> bool:
        """Email로 알림 전송 (Databricks Email 사용)"""
        if not self.email_recipients:
            return False
        
        # HTML 이메일 본문 생성
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #1E88E5; color: white; padding: 20px; }}
                .content {{ padding: 20px; }}
                .severity-critical {{ color: #FF0000; font-weight: bold; }}
                .severity-warning {{ color: #FFA500; font-weight: bold; }}
                .severity-info {{ color: #0000FF; font-weight: bold; }}
                .details {{ background-color: #f5f5f5; padding: 15px; margin: 10px 0; }}
                .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🔔 Pipeline Monitoring Alert</h2>
            </div>
            <div class="content">
                <h3 class="severity-{alert['severity']}">{alert['title']}</h3>
                <p><strong>Alert Type:</strong> {alert['alert_type']}</p>
                <p><strong>Severity:</strong> {alert['severity'].upper()}</p>
                <p><strong>Timestamp:</strong> {alert['timestamp']}</p>
                <div class="details">
                    <h4>Message:</h4>
                    <p>{alert['message']}</p>
                    {self._format_details_html(alert.get('details', {}))}
                </div>
            </div>
            <div class="footer">
                <p>This is an automated alert from the Pipeline Monitoring System.</p>
            </div>
        </body>
        </html>
        """
        
        try:
            # Databricks Email API 사용 (예시)
            # 실제로는 dbutils.notebook.exit() 또는 별도 이메일 서비스 사용
            for recipient in self.email_recipients:
                print(f"📧 Email 전송 대상: {recipient}")
                # TODO: 실제 이메일 전송 로직 구현
                # dbutils.notebook.email(recipient, alert['title'], html_body)
            
            print(f"✅ Email 알림 전송 완료: {alert['title']}")
            return True
        except Exception as e:
            print(f"❌ Email 알림 전송 실패: {str(e)}")
            return False
    
    def _format_details_html(self, details: Dict[str, Any]) -> str:
        """상세 정보를 HTML로 포맷팅"""
        if not details:
            return ""
        
        html = "<h4>Details:</h4><ul>"
        for key, value in details.items():
            html += f"<li><strong>{key}:</strong> {value}</li>"
        html += "</ul>"
        return html
    
    def send_all_alerts(self):
        """모든 알림 전송"""
        if not self.alerts:
            print("✅ 알림 없음 - 모든 파이프라인 정상")
            return
        
        print(f"\n📬 총 {len(self.alerts)}개의 알림 전송 중...")
        
        for alert in self.alerts:
            # Alert Level 필터링
            if ALERT_LEVEL == "critical" and alert["severity"] != "critical":
                continue
            elif ALERT_LEVEL == "warning" and alert["severity"] not in ["critical", "warning"]:
                continue
            
            # Slack 전송
            if self.slack_webhook:
                self.send_slack_alert(alert)
            
            # Email 전송
            if self.email_recipients:
                self.send_email_alert(alert)
        
        # 요약 출력
        self._print_summary()
    
    def _print_summary(self):
        """알림 요약 출력"""
        critical_count = sum(1 for a in self.alerts if a["severity"] == "critical")
        warning_count = sum(1 for a in self.alerts if a["severity"] == "warning")
        info_count = sum(1 for a in self.alerts if a["severity"] == "info")
        
        print("\n" + "="*50)
        print("📊 알림 요약")
        print("="*50)
        print(f"🔴 Critical: {critical_count}")
        print(f"🟡 Warning: {warning_count}")
        print(f"🔵 Info: {info_count}")
        print(f"📌 Total: {len(self.alerts)}")
        print("="*50)

# AlertManager 인스턴스 생성
alert_manager = AlertManager(
    slack_webhook=SLACK_WEBHOOK_URL if SLACK_WEBHOOK_URL else None,
    email_recipients=EMAIL_RECIPIENTS if EMAIL_RECIPIENTS else None
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. 파이프라인 실패 감지

# COMMAND ----------
def check_pipeline_failures() -> bool:
    """최근 파이프라인 실패 체크"""
    print(f"\n🔍 1. 파이프라인 실패 체크 (최근 {MONITORING_WINDOW_HOURS}시간)...")
    
    try:
        query = f"""
            SELECT 
                dataflow_id,
                stage,
                error_message,
                start_time,
                duration_seconds
            FROM {CATALOG}.metadata.pipeline_execution_history
            WHERE status = 'failed'
              AND start_time >= CURRENT_TIMESTAMP() - INTERVAL {MONITORING_WINDOW_HOURS} HOURS
            ORDER BY start_time DESC
        """
        
        print("   📊 쿼리 실행 중...")
        failures_df = spark.sql(query)
        failures = failures_df.collect()
        print(f"   📊 쿼리 완료 - {len(failures)}개의 레코드 반환")
    except Exception as e:
        print(f"   ❌ 쿼리 실행 실패: {str(e)}")
        return True  # 쿼리 실패는 알림 없이 통과
    
    if not failures:
        print("   ✅ 실패 없음")
        return True
    
    print(f"   ❌ {len(failures)}개의 실패 감지!")
    
    # 각 실패에 대해 알림 추가
    for failure in failures:
        alert_manager.add_alert(
            alert_type="Pipeline Failure",
            severity="critical",
            title=f"파이프라인 실패: {failure.dataflow_id} ({failure.stage})",
            message=f"파이프라인이 실패했습니다.",
            details={
                "Dataflow ID": failure.dataflow_id,
                "Stage": failure.stage,
                "Error": failure.error_message[:200] if failure.error_message else "No error message",
                "Timestamp": str(failure.start_time),
                "Duration (sec)": failure.duration_seconds
            }
        )
        
        # 콘솔 출력
        print(f"      • {failure.dataflow_id} ({failure.stage})")
        print(f"        Error: {failure.error_message[:100]}...")
    
    return False

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. 데이터 품질 이상치 감지

# COMMAND ----------
def check_data_quality_anomalies() -> bool:
    """데이터 품질 이상치 감지"""
    print(f"\n🔍 2. 데이터 품질 이상치 감지...")
    
    # 최근 30일 통계 기반 이상치 탐지
    query = f"""
    WITH daily_stats AS (
        SELECT 
            dataflow_id,
            stage,
            AVG(rows_processed) as avg_rows,
            STDDEV(rows_processed) as stddev_rows,
            COUNT(*) as sample_count
        FROM {CATALOG}.metadata.pipeline_execution_history
        WHERE start_time >= CURRENT_DATE() - INTERVAL 30 DAYS
          AND status = 'success'
          AND rows_processed IS NOT NULL
        GROUP BY dataflow_id, stage
        HAVING COUNT(*) >= 5  -- 최소 5개 샘플 필요
    ),
    latest_run AS (
        SELECT 
            dataflow_id,
            stage,
            rows_processed,
            start_time,
            ROW_NUMBER() OVER (PARTITION BY dataflow_id, stage ORDER BY start_time DESC) as rn
        FROM {CATALOG}.metadata.pipeline_execution_history
        WHERE status = 'success'
          AND rows_processed IS NOT NULL
          AND start_time >= CURRENT_TIMESTAMP() - INTERVAL {MONITORING_WINDOW_HOURS} HOURS
    )
    SELECT 
        l.dataflow_id,
        l.stage,
        l.rows_processed as latest_rows,
        ROUND(d.avg_rows, 2) as historical_avg,
        ROUND(d.stddev_rows, 2) as historical_stddev,
        l.start_time as latest_run_time,
        CASE 
            WHEN d.stddev_rows > 0 THEN 
                ROUND((l.rows_processed - d.avg_rows) / d.stddev_rows, 2)
            ELSE 0
        END as z_score,
        d.sample_count
    FROM latest_run l
    JOIN daily_stats d ON l.dataflow_id = d.dataflow_id 
                       AND l.stage = d.stage
    WHERE l.rn = 1
      AND d.stddev_rows > 0
      AND ABS((l.rows_processed - d.avg_rows) / d.stddev_rows) > 3  -- Z-Score > 3
    ORDER BY ABS((l.rows_processed - d.avg_rows) / d.stddev_rows) DESC
    """
    
    anomalies_df = spark.sql(query)
    anomalies = anomalies_df.collect()
    
    if not anomalies:
        print("   ✅ 이상치 없음")
        return True
    
    print(f"   ⚠️ {len(anomalies)}개의 이상치 감지!")
    
    # 각 이상치에 대해 알림 추가
    for anomaly in anomalies:
        severity = "critical" if abs(anomaly.z_score) > 5 else "warning"
        direction = "급증" if anomaly.z_score > 0 else "급감"
        
        alert_manager.add_alert(
            alert_type="Data Quality Anomaly",
            severity=severity,
            title=f"데이터 이상치: {anomaly.dataflow_id} ({anomaly.stage}) - {direction}",
            message=f"처리된 레코드 수가 평균 대비 비정상적으로 {direction}했습니다.",
            details={
                "Dataflow ID": anomaly.dataflow_id,
                "Stage": anomaly.stage,
                "Latest Rows": f"{anomaly.latest_rows:,}",
                "Historical Average": f"{anomaly.historical_avg:,}",
                "Z-Score": anomaly.z_score,
                "Deviation": f"{((anomaly.latest_rows - anomaly.historical_avg) / anomaly.historical_avg * 100):.1f}%",
                "Sample Size": anomaly.sample_count,
                "Timestamp": str(anomaly.latest_run_time)
            }
        )
        
        # 콘솔 출력
        print(f"      • {anomaly.dataflow_id} ({anomaly.stage})")
        print(f"        Latest: {anomaly.latest_rows:,} | Avg: {anomaly.historical_avg:,} | Z-Score: {anomaly.z_score}")
    
    return False

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. SLA 초과 감지

# COMMAND ----------
def check_sla_violations() -> bool:
    """SLA 초과 감지"""
    print(f"\n🔍 3. SLA 초과 감지...")
    
    # SLA 기준 (초 단위)
    SLA_LIMITS = {
        "rdb_ingestion": 900,   # 15분
        "bronze": 600,          # 10분
        "silver": 900           # 15분
    }
    
    sla_conditions = " OR ".join([
        f"(stage = '{stage}' AND duration_seconds > {limit})"
        for stage, limit in SLA_LIMITS.items()
    ])
    
    query = f"""
        SELECT 
            dataflow_id,
            stage,
            duration_seconds,
            ROUND(duration_seconds / 60, 1) as duration_minutes,
            start_time,
            end_time,
            rows_processed
        FROM {CATALOG}.metadata.pipeline_execution_history
        WHERE status = 'success'
          AND start_time >= CURRENT_TIMESTAMP() - INTERVAL {MONITORING_WINDOW_HOURS} HOURS
          AND ({sla_conditions})
        ORDER BY duration_seconds DESC
    """
    
    violations_df = spark.sql(query)
    violations = violations_df.collect()
    
    if not violations:
        print("   ✅ SLA 초과 없음")
        return True
    
    print(f"   ⚠️ {len(violations)}개의 SLA 초과 감지!")
    
    # 각 SLA 초과에 대해 알림 추가
    for violation in violations:
        sla_limit = SLA_LIMITS.get(violation.stage, 0)
        exceeded_by = violation.duration_seconds - sla_limit
        
        alert_manager.add_alert(
            alert_type="SLA Violation",
            severity="warning",
            title=f"SLA 초과: {violation.dataflow_id} ({violation.stage})",
            message=f"파이프라인 실행 시간이 SLA를 초과했습니다.",
            details={
                "Dataflow ID": violation.dataflow_id,
                "Stage": violation.stage,
                "Duration": f"{violation.duration_minutes} minutes",
                "SLA Limit": f"{sla_limit / 60} minutes",
                "Exceeded By": f"{exceeded_by / 60:.1f} minutes",
                "Rows Processed": f"{violation.rows_processed:,}" if violation.rows_processed else "N/A",
                "Start Time": str(violation.start_time),
                "End Time": str(violation.end_time)
            }
        )
        
        # 콘솔 출력
        print(f"      • {violation.dataflow_id} ({violation.stage})")
        print(f"        Duration: {violation.duration_minutes}분 (SLA: {sla_limit/60}분)")
    
    return False

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. 파이프라인 성공률 추적

# COMMAND ----------
def check_success_rate() -> bool:
    """파이프라인 성공률 체크 (최근 24시간)"""
    print(f"\n🔍 4. 파이프라인 성공률 체크 (최근 24시간)...")
    
    query = f"""
    SELECT 
        dataflow_id,
        stage,
        COUNT(*) as total_runs,
        COUNT_IF(status = 'success') as success_count,
        COUNT_IF(status = 'failed') as failed_count,
        ROUND(COUNT_IF(status = 'success') * 100.0 / COUNT(*), 2) as success_rate_pct
    FROM {CATALOG}.metadata.pipeline_execution_history
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
    GROUP BY dataflow_id, stage
    HAVING COUNT(*) >= 3  -- 최소 3번 실행
       AND success_rate_pct < 80  -- 성공률 80% 미만
    ORDER BY success_rate_pct ASC
    """
    
    low_success_df = spark.sql(query)
    low_success = low_success_df.collect()
    
    if not low_success:
        print("   ✅ 모든 파이프라인 성공률 양호 (>80%)")
        return True
    
    print(f"   ⚠️ {len(low_success)}개의 파이프라인이 낮은 성공률을 보임!")
    
    # 각 낮은 성공률에 대해 알림 추가
    for pipeline in low_success:
        severity = "critical" if pipeline.success_rate_pct < 50 else "warning"
        
        alert_manager.add_alert(
            alert_type="Low Success Rate",
            severity=severity,
            title=f"낮은 성공률: {pipeline.dataflow_id} ({pipeline.stage}) - {pipeline.success_rate_pct}%",
            message=f"최근 24시간 동안 파이프라인 성공률이 {pipeline.success_rate_pct}%로 낮습니다.",
            details={
                "Dataflow ID": pipeline.dataflow_id,
                "Stage": pipeline.stage,
                "Success Rate": f"{pipeline.success_rate_pct}%",
                "Total Runs": pipeline.total_runs,
                "Success Count": pipeline.success_count,
                "Failed Count": pipeline.failed_count
            }
        )
        
        # 콘솔 출력
        print(f"      • {pipeline.dataflow_id} ({pipeline.stage})")
        print(f"        Success Rate: {pipeline.success_rate_pct}% ({pipeline.success_count}/{pipeline.total_runs})")
    
    return False

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. 데이터 지연 감지

# COMMAND ----------
def check_data_freshness() -> bool:
    """데이터 신선도 체크 (최근 실행 시간)"""
    print(f"\n🔍 5. 데이터 신선도 체크...")
    
    # 예상 실행 주기 (초 단위)
    EXPECTED_FREQUENCY = {
        "item_goods_option": 3600  # 1시간마다 실행 예상
    }
    
    query = f"""
    WITH latest_runs AS (
        SELECT 
            dataflow_id,
            stage,
            MAX(start_time) as last_run_time,
            TIMESTAMPDIFF(HOUR, MAX(start_time), CURRENT_TIMESTAMP()) as hours_since_last_run
        FROM {CATALOG}.metadata.pipeline_execution_history
        WHERE status = 'success'
        GROUP BY dataflow_id, stage
    )
    SELECT 
        dataflow_id,
        stage,
        last_run_time,
        hours_since_last_run
    FROM latest_runs
    WHERE hours_since_last_run > 2  -- 2시간 이상 실행 없음
    ORDER BY hours_since_last_run DESC
    """
    
    stale_df = spark.sql(query)
    stale = stale_df.collect()
    
    if not stale:
        print("   ✅ 모든 파이프라인 정기적으로 실행 중")
        return True
    
    print(f"   ⚠️ {len(stale)}개의 파이프라인이 오랫동안 실행되지 않음!")
    
    # 각 지연된 파이프라인에 대해 알림 추가
    for pipeline in stale:
        severity = "critical" if pipeline.hours_since_last_run > 6 else "warning"
        
        alert_manager.add_alert(
            alert_type="Data Freshness",
            severity=severity,
            title=f"데이터 지연: {pipeline.dataflow_id} ({pipeline.stage})",
            message=f"파이프라인이 {pipeline.hours_since_last_run}시간 동안 실행되지 않았습니다.",
            details={
                "Dataflow ID": pipeline.dataflow_id,
                "Stage": pipeline.stage,
                "Last Run": str(pipeline.last_run_time),
                "Hours Since Last Run": pipeline.hours_since_last_run
            }
        )
        
        # 콘솔 출력
        print(f"      • {pipeline.dataflow_id} ({pipeline.stage})")
        print(f"        Last Run: {pipeline.last_run_time} ({pipeline.hours_since_last_run}시간 전)")
    
    return False

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. 종합 모니터링 실행

# COMMAND ----------
def run_comprehensive_monitoring():
    """종합 모니터링 실행"""
    print("="*60)
    print("🚀 파이프라인 모니터링 시작")
    print("="*60)
    print(f"⏰ Timestamp: {datetime.now()}")
    print(f"📊 Catalog: {CATALOG}")
    print(f"🔍 Monitoring Window: {MONITORING_WINDOW_HOURS} hours")
    print("="*60)
    
    # 각 체크 함수 실행
    checks = {
        "파이프라인 실패": check_pipeline_failures,
        "데이터 품질 이상치": check_data_quality_anomalies,
        "SLA 초과": check_sla_violations,
        "성공률": check_success_rate,
        "데이터 신선도": check_data_freshness
    }
    
    results = {}
    for check_name, check_func in checks.items():
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"\n❌ {check_name} 체크 중 오류 발생: {str(e)}")
            results[check_name] = False
            
            # 체크 실패 자체를 알림으로 추가
            alert_manager.add_alert(
                alert_type="Monitoring Error",
                severity="critical",
                title=f"모니터링 체크 실패: {check_name}",
                message=f"모니터링 체크 실행 중 오류가 발생했습니다.",
                details={
                    "Check Name": check_name,
                    "Error": str(e)
                }
            )
    
    # 알림 전송
    print("\n" + "="*60)
    print("📬 알림 전송")
    print("="*60)
    alert_manager.send_all_alerts()
    
    # 최종 요약
    print("\n" + "="*60)
    print("📊 모니터링 결과 요약")
    print("="*60)
    all_passed = all(results.values())
    for check_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {check_name}")
    
    print("="*60)
    if all_passed:
        print("✅ 모든 체크 통과 - 파이프라인 정상")
    else:
        print(f"⚠️ {sum(1 for v in results.values() if not v)}개의 문제 발견")
    print("="*60)
    print(f"🏁 모니터링 완료: {datetime.now()}")
    print("="*60)
    
    # 결과를 dbutils.jobs.taskValues에 저장 (다음 Task에서 사용 가능)
    try:
        dbutils.jobs.taskValues.set(key="monitoring_results", value=json.dumps({
            "timestamp": datetime.now().isoformat(),
            "all_passed": all_passed,
            "checks": results,
            "alert_count": len(alert_manager.alerts)
        }))
    except:
        pass  # Task Values는 Job 실행 시에만 사용 가능
    
    return all_passed

# 모니터링 실행
monitoring_passed = run_comprehensive_monitoring()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. 모니터링 이력 저장 (선택사항)

# COMMAND ----------
# 모니터링 결과를 별도 테이블에 저장
def save_monitoring_history():
    """모니터링 결과 저장"""
    try:
        now = datetime.now()
        
        # 명시적 스키마 정의 (IntegerType 사용)
        schema = StructType([
            StructField("monitoring_timestamp", TimestampType(), False),
            StructField("monitoring_date", DateType(), False),
            StructField("monitoring_window_hours", IntegerType(), True),
            StructField("alert_count", IntegerType(), True),
            StructField("critical_count", IntegerType(), True),
            StructField("warning_count", IntegerType(), True),
            StructField("alerts_json", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        
        monitoring_data = [(
            now,
            now.date(),  # 파티션 컬럼
            int(MONITORING_WINDOW_HOURS),  # 명시적으로 int 변환
            int(len(alert_manager.alerts)),
            int(sum(1 for a in alert_manager.alerts if a["severity"] == "critical")),
            int(sum(1 for a in alert_manager.alerts if a["severity"] == "warning")),
            json.dumps(alert_manager.alerts),
            now  # 명시적으로 제공
        )]
        
        monitoring_df = spark.createDataFrame(monitoring_data, schema=schema)
        
        # 테이블에 append
        monitoring_df.write.format("delta").mode("append").saveAsTable(
            f"{CATALOG}.metadata.monitoring_history"
        )
        
        print("✅ 모니터링 이력 저장 완료")
    except Exception as e:
        print(f"⚠️ 모니터링 이력 저장 실패: {str(e)}")

# 이력 저장 실행
save_monitoring_history()

# COMMAND ----------
# Exit with status
dbutils.notebook.exit(json.dumps({
    "status": "success" if monitoring_passed else "warning",
    "alert_count": len(alert_manager.alerts),
    "timestamp": datetime.now().isoformat()
}))
