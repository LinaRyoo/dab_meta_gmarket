# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validator
# MAGIC 
# MAGIC DLT 파이프라인 실행 후 데이터 품질을 검증하는 노트북입니다.
# MAGIC 
# MAGIC **검증 항목:**
# MAGIC - 테이블 존재 여부
# MAGIC - 레코드 수 확인
# MAGIC - 필수 컬럼 존재 확인
# MAGIC - NULL 값 검증
# MAGIC - 중복 데이터 확인

# COMMAND ----------
# MAGIC %md ## Parameters

# COMMAND ----------
dbutils.widgets.text("dataflow_id", "", "Dataflow ID")
dbutils.widgets.text("catalog", "baikald1ws", "Catalog Name")

dataflow_id = dbutils.widgets.get("dataflow_id")
catalog = dbutils.widgets.get("catalog")

print("="*70)
print("Data Quality Validator")
print("="*70)
print(f"Dataflow ID: {dataflow_id}")
print(f"Catalog: {catalog}")
print("="*70)

# COMMAND ----------
# MAGIC %md ## Import Libraries

# COMMAND ----------
from pyspark.sql import functions as F
from datetime import datetime
import json

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------
# 검증할 테이블 정의
tables_to_validate = {
    "raw_bronze": f"{catalog}.sdp_poc.{dataflow_id}_rdb_raw",
    "bronze": f"{catalog}.sdp_poc.{dataflow_id}_bronze",
    "silver": f"{catalog}.sdp_poc.{dataflow_id}_silver"
}

validation_results = []
overall_status = "PASSED"

# COMMAND ----------
# MAGIC %md ## Validation Functions

# COMMAND ----------
def validate_table_exists(table_name):
    """테이블 존재 여부 확인"""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True, "Table exists"
    except Exception as e:
        return False, f"Table does not exist: {str(e)}"

def validate_record_count(table_name, min_records=1):
    """레코드 수 확인"""
    try:
        count = spark.table(table_name).count()
        if count >= min_records:
            return True, f"Record count: {count:,}"
        else:
            return False, f"Insufficient records: {count} < {min_records}"
    except Exception as e:
        return False, f"Error counting records: {str(e)}"

def validate_required_columns(table_name, required_columns):
    """필수 컬럼 존재 확인"""
    try:
        df = spark.table(table_name)
        existing_columns = set(df.columns)
        missing_columns = set(required_columns) - existing_columns
        
        if not missing_columns:
            return True, f"All required columns exist: {required_columns}"
        else:
            return False, f"Missing columns: {missing_columns}"
    except Exception as e:
        return False, f"Error validating columns: {str(e)}"

def validate_null_values(table_name, non_null_columns):
    """NULL 값 검증"""
    try:
        df = spark.table(table_name)
        null_counts = {}
        
        for col in non_null_columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    null_counts[col] = null_count
        
        if not null_counts:
            return True, f"No NULL values in required columns: {non_null_columns}"
        else:
            return False, f"NULL values found: {null_counts}"
    except Exception as e:
        return False, f"Error validating NULL values: {str(e)}"

def validate_duplicates(table_name, key_columns):
    """중복 데이터 확인"""
    try:
        df = spark.table(table_name)
        
        if not all(col in df.columns for col in key_columns):
            return True, f"Key columns not found, skipping duplicate check"
        
        duplicate_count = df.groupBy(*key_columns).count().filter("count > 1").count()
        
        if duplicate_count == 0:
            return True, f"No duplicates found on keys: {key_columns}"
        else:
            return False, f"Duplicates found: {duplicate_count} records"
    except Exception as e:
        return False, f"Error checking duplicates: {str(e)}"

# COMMAND ----------
# MAGIC %md ## Validate Raw Bronze Table

# COMMAND ----------
print("\n" + "="*70)
print("🔍 Validating Raw Bronze Table")
print("="*70)

raw_bronze_table = tables_to_validate["raw_bronze"]

# 1. 테이블 존재 확인
status, message = validate_table_exists(raw_bronze_table)
validation_results.append({
    "table": "raw_bronze",
    "check": "table_exists",
    "status": "PASSED" if status else "FAILED",
    "message": message
})
print(f"{'✅' if status else '❌'} Table Exists: {message}")

if status:
    # 2. 레코드 수 확인
    status, message = validate_record_count(raw_bronze_table)
    validation_results.append({
        "table": "raw_bronze",
        "check": "record_count",
        "status": "PASSED" if status else "FAILED",
        "message": message
    })
    print(f"{'✅' if status else '❌'} Record Count: {message}")
    
    # 3. 메타데이터 컬럼 확인
    required_cols = ["ingest_dt", "_source_system", "_dataflow_id"]
    status, message = validate_required_columns(raw_bronze_table, required_cols)
    validation_results.append({
        "table": "raw_bronze",
        "check": "metadata_columns",
        "status": "PASSED" if status else "FAILED",
        "message": message
    })
    print(f"{'✅' if status else '❌'} Metadata Columns: {message}")

# COMMAND ----------
# MAGIC %md ## Validate Bronze Table

# COMMAND ----------
print("\n" + "="*70)
print("🔍 Validating Bronze Table")
print("="*70)

bronze_table = tables_to_validate["bronze"]

# 1. 테이블 존재 확인
status, message = validate_table_exists(bronze_table)
validation_results.append({
    "table": "bronze",
    "check": "table_exists",
    "status": "PASSED" if status else "FAILED",
    "message": message
})
print(f"{'✅' if status else '❌'} Table Exists: {message}")

if status:
    # 2. 레코드 수 확인
    status, message = validate_record_count(bronze_table)
    validation_results.append({
        "table": "bronze",
        "check": "record_count",
        "status": "PASSED" if status else "FAILED",
        "message": message
    })
    print(f"{'✅' if status else '❌'} Record Count: {message}")

# COMMAND ----------
# MAGIC %md ## Validate Silver Table

# COMMAND ----------
print("\n" + "="*70)
print("🔍 Validating Silver Table")
print("="*70)

silver_table = tables_to_validate["silver"]

# 1. 테이블 존재 확인
status, message = validate_table_exists(silver_table)
validation_results.append({
    "table": "silver",
    "check": "table_exists",
    "status": "PASSED" if status else "FAILED",
    "message": message
})
print(f"{'✅' if status else '❌'} Table Exists: {message}")

if status:
    # 2. 레코드 수 확인
    status, message = validate_record_count(silver_table)
    validation_results.append({
        "table": "silver",
        "check": "record_count",
        "status": "PASSED" if status else "FAILED",
        "message": message
    })
    print(f"{'✅' if status else '❌'} Record Count: {message}")
    
    # 3. Primary Key NULL 검증
    non_null_cols = ["OPT_NO"]  # 실제 키 컬럼으로 변경
    status, message = validate_null_values(silver_table, non_null_cols)
    validation_results.append({
        "table": "silver",
        "check": "null_values",
        "status": "PASSED" if status else "FAILED",
        "message": message
    })
    print(f"{'✅' if status else '❌'} NULL Values: {message}")
    
    # 4. 중복 확인
    key_cols = ["OPT_NO"]  # 실제 키 컬럼으로 변경
    status, message = validate_duplicates(silver_table, key_cols)
    validation_results.append({
        "table": "silver",
        "check": "duplicates",
        "status": "PASSED" if status else "FAILED",
        "message": message
    })
    print(f"{'✅' if status else '❌'} Duplicates: {message}")

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------
# 전체 결과 집계
total_checks = len(validation_results)
passed_checks = len([r for r in validation_results if r["status"] == "PASSED"])
failed_checks = total_checks - passed_checks

overall_status = "PASSED" if failed_checks == 0 else "FAILED"

print("\n" + "="*70)
print("📊 VALIDATION SUMMARY")
print("="*70)
print(f"Dataflow ID: {dataflow_id}")
print(f"Total Checks: {total_checks}")
print(f"Passed: {passed_checks}")
print(f"Failed: {failed_checks}")
print(f"Overall Status: {overall_status}")
print("="*70)

# 실패한 검증 출력
if failed_checks > 0:
    print("\n❌ Failed Validations:")
    for result in validation_results:
        if result["status"] == "FAILED":
            print(f"  - {result['table']}.{result['check']}: {result['message']}")

# 결과를 JSON으로 반환
result_json = {
    "dataflow_id": dataflow_id,
    "timestamp": datetime.now().isoformat(),
    "overall_status": overall_status,
    "total_checks": total_checks,
    "passed_checks": passed_checks,
    "failed_checks": failed_checks,
    "validations": validation_results
}

dbutils.notebook.exit(json.dumps(result_json))
