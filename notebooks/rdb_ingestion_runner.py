# Databricks notebook source
# MAGIC %md
# MAGIC # RDB Ingestion Runner
# MAGIC 
# MAGIC 통합 메타데이터 기반 RDB 데이터 추출 및 Raw Bronze 저장
# MAGIC 
# MAGIC **Features:**
# MAGIC - SQL Server prepare_query + main_query 지원
# MAGIC - Databricks Secrets 연동
# MAGIC - 메타데이터 추적 (lineage)
# MAGIC - 동적 파라미터 치환

# COMMAND ----------
# MAGIC %md ## Parameters

# COMMAND ----------
dbutils.widgets.text("config_file", "", "Config File Path")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("trigger_time", "", "Trigger Time (ISO format)")

config_file = dbutils.widgets.get("config_file")
environment = dbutils.widgets.get("environment")
trigger_time_str = dbutils.widgets.get("trigger_time")

print("="*70)
print("RDB Ingestion Runner")
print("="*70)
print(f"Config File: {config_file}")
print(f"Environment: {environment}")
print(f"Trigger Time: {trigger_time_str}")
print("="*70)

# COMMAND ----------
# MAGIC %md ## Load Configuration

# COMMAND ----------
import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
import uuid

# Config 로드
with open(config_file, 'r') as f:
    config = json.load(f)

dataflow_id = config["dataflow_id"]
print(f"\n📦 Processing dataflow: {dataflow_id}")
print(f"   Description: {config['description']}")

# Trigger time 처리
if trigger_time_str:
    trigger_time = datetime.fromisoformat(trigger_time_str)
else:
    trigger_time = datetime.now()

print(f"   Trigger Time: {trigger_time.isoformat()}")

# COMMAND ----------
# MAGIC %md ## Build JDBC Connection

# COMMAND ----------
source = config["source"]
connection = source["connection"]

# Secrets에서 인증 정보 가져오기
secrets_scope = connection["secrets"]["scope"]
username = dbutils.secrets.get(secrets_scope, connection["secrets"]["username_key"])
password = dbutils.secrets.get(secrets_scope, connection["secrets"]["password_key"])

# JDBC URL
jdbc_url = connection["jdbc_url"]
driver = connection["driver"]

print(f"\n🔌 JDBC Connection:")
print(f"   URL: {jdbc_url}")
print(f"   Driver: {driver}")
print(f"   Username: {username[:3]}***")

# COMMAND ----------
# MAGIC %md ## Prepare SQL Query

# COMMAND ----------
extraction = source["extraction"]
parameters = extraction.get("parameters", {})

# 파라미터 치환
def replace_parameters(query: str, trigger_time: datetime) -> str:
    """쿼리 내 파라미터를 실제 값으로 치환"""
    replacements = {}
    
    # 기본 시간 파라미터 (SQL Server 호환 형식)
    # SQL Server: 'YYYY-MM-DD HH:MM:SS.mmm' 형식 선호
    def to_sql_datetime(dt):
        """datetime을 SQL Server 친화적 형식으로 변환"""
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # 밀리초까지만
    
    replacements["${trigger_time.isoformat()}"] = to_sql_datetime(trigger_time)
    replacements["${trigger_time - timedelta(days=1)}"] = to_sql_datetime(trigger_time - timedelta(days=1))
    replacements["${current_date()}"] = trigger_time.strftime('%Y-%m-%d')
    
    # Custom 파라미터 처리
    for param_name, param_config in parameters.items():
        if "runtime_value" in param_config:
            # Python 표현식 평가 (${} wrapper 제거)
            expr = param_config["runtime_value"]
            if expr.startswith("${") and expr.endswith("}"):
                expr = expr[2:-1]  # ${...} → ...
            # eval에 필요한 변수들을 scope에 제공
            runtime_value = eval(expr, {"trigger_time": trigger_time, "timedelta": timedelta})
            # datetime 객체는 SQL Server 형식으로 변환
            if isinstance(runtime_value, datetime):
                runtime_value = to_sql_datetime(runtime_value)
            elif isinstance(runtime_value, timedelta):
                runtime_value = str(runtime_value)
            elif hasattr(runtime_value, 'isoformat'):
                # date 객체 등
                runtime_value = runtime_value.isoformat()
        else:
            runtime_value = param_config.get("default", "")
        
        replacements[f"${{{param_name}}}"] = str(runtime_value)
    
    result = query
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)
    
    return result

# 쿼리 빌드
prepare_query = extraction.get("prepare_query", "")
main_query = extraction["main_query"]

prepare_query = replace_parameters(prepare_query, trigger_time)
main_query = replace_parameters(main_query, trigger_time)

# SQL Server의 경우 READ UNCOMMITTED 추가 (WITH NOLOCK과 유사한 효과)
db_type = connection.get("db_type", "mssql").lower()
if db_type == "mssql" and prepare_query:
    prepare_query = f"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; {prepare_query}"

print(f"\n📝 SQL Query prepared:")
print(f"   Prepare query length: {len(prepare_query)} chars")
if prepare_query:
    print(f"   ✓ Prepare query will be executed via prepareQuery option")
    print(f"   Prepare query:\n{prepare_query}")
print(f"   Main query length: {len(main_query)} chars")
print(f"   Main query:\n{main_query}")

# COMMAND ----------
# MAGIC %md ## Execute Query & Extract Data

# COMMAND ----------
execution_id = str(uuid.uuid4())
start_time = datetime.now()

print(f"\n🚀 Execution ID: {execution_id}")
print(f"   Start Time: {start_time.isoformat()}")

try:
    # JDBC 읽기
    read_options = extraction.get("read_options", {})
    
    # prepareQuery: 메인 쿼리 실행 직전에 같은 연결에서 실행됨 (임시 테이블 생성 가능)
    jdbc_reader = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", username) \
        .option("password", password) \
        .option("driver", driver) \
        .options(**read_options)
    
    # prepare_query가 있으면 prepareQuery 옵션으로 실행
    if prepare_query:
        jdbc_reader = jdbc_reader.option("prepareQuery", prepare_query)
    
    df = jdbc_reader \
        .option("query", main_query) \
        .load()
    
    row_count = df.count()
    print(f"\n✅ Data extracted successfully!")
    print(f"   Rows: {row_count:,}")
    print(f"   Columns: {len(df.columns)}")
    
    # 샘플 데이터 출력
    print(f"\n📊 Sample data:")
    df.show(5, truncate=False)
    
except Exception as e:
    print(f"\n❌ Error extracting data: {str(e)}")
    raise

# COMMAND ----------
# MAGIC %md ## Add Metadata Columns

# COMMAND ----------
target = config["target"]
metadata_columns = target.get("metadata_columns", {})

# 메타데이터 컬럼 추가
for col_name, col_expression in metadata_columns.items():
    if col_expression == "current_timestamp()":
        df = df.withColumn(col_name, F.current_timestamp())
    elif col_expression.startswith("${") and col_expression.endswith("}"):
        # 동적 값 평가 (${} wrapper 제거 및 scope 제공)
        expr = col_expression[2:-1]  # ${...} → ...
        value = eval(expr, {
            "trigger_time": trigger_time, 
            "timedelta": timedelta,
            "execution_id": execution_id,
            "dataflow_id": dataflow_id
        })
        df = df.withColumn(col_name, F.lit(value))
    else:
        # 리터럴 값
        df = df.withColumn(col_name, F.lit(col_expression))

print(f"\n✅ Metadata columns added:")
for col_name in metadata_columns.keys():
    print(f"   - {col_name}")

# 최종 스키마
print(f"\n📋 Final schema:")
df.printSchema()

# COMMAND ----------
# MAGIC %md ## Write to Raw Bronze

# COMMAND ----------
target_catalog = target["catalog"]
target_schema = target["schema"]
target_table = target["table"]
target_full_name = f"{target_catalog}.{target_schema}.{target_table}"

write_mode = target.get("write_mode", "append")
partition_columns = target.get("partition_columns", [])
table_properties = target.get("table_properties", {})

print(f"\n💾 Writing to: {target_full_name}")
print(f"   Mode: {write_mode}")
print(f"   Partitions: {partition_columns}")
print(f"   Properties: {table_properties}")

try:
    writer = df.write \
        .format(target["format"]) \
        .mode(write_mode) \
        .option("mergeSchema", "true")
    
    # Table properties 적용
    for prop_key, prop_value in table_properties.items():
        writer = writer.option(prop_key, prop_value)
    
    # Partition 적용
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    
    # 저장
    writer.saveAsTable(target_full_name)
    
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    
    print(f"\n✅ Data written successfully!")
    print(f"   Rows written: {row_count:,}")
    print(f"   Duration: {duration_seconds:.2f} seconds")
    print(f"   Throughput: {row_count / duration_seconds:.2f} rows/sec")
    
    # 결과 반환
    result = {
        "status": "success",
        "execution_id": execution_id,
        "dataflow_id": dataflow_id,
        "rows_processed": row_count,
        "duration_seconds": duration_seconds,
        "target_table": target_full_name,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat()
    }
    
except Exception as e:
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    
    print(f"\n❌ Error writing data: {str(e)}")
    
    result = {
        "status": "failed",
        "execution_id": execution_id,
        "dataflow_id": dataflow_id,
        "error_message": str(e),
        "duration_seconds": duration_seconds,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat()
    }
    
    raise

# COMMAND ----------
# MAGIC %md ## Record Execution Metadata

# COMMAND ----------
# 메타데이터 테이블에 실행 이력 기록
metadata_catalog = target_catalog
metadata_table = f"{metadata_catalog}.metadata.pipeline_execution_history"

# 테이블이 존재하는지 확인
try:
    execution_record = spark.createDataFrame([{
        "execution_id": execution_id,
        "dataflow_id": dataflow_id,
        "dataflow_group": config.get("dataflow_group", ""),
        "stage": "rdb_ingestion",
        "status": result["status"],
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration_seconds,
        "rows_processed": row_count if result["status"] == "success" else None,
        "error_message": result.get("error_message"),
        "metadata": {
            "source_system": connection["connection_name"],
            "target_table": target_full_name,
            "environment": environment
        }
    }])
    
    execution_record.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(metadata_table)
    
    print(f"\n📝 Execution metadata recorded to: {metadata_table}")
    
except Exception as e:
    print(f"\n⚠️  Warning: Could not record metadata: {str(e)}")
    # 메타데이터 기록 실패는 무시 (메인 작업은 성공)

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------
print("\n" + "="*70)
print("📊 EXECUTION SUMMARY")
print("="*70)
print(f"Dataflow ID: {dataflow_id}")
print(f"Execution ID: {execution_id}")
print(f"Status: {result['status'].upper()}")
print(f"Rows Processed: {result.get('rows_processed', 'N/A'):,}")
print(f"Duration: {result['duration_seconds']:.2f} seconds")
print(f"Target Table: {target_full_name}")
print("="*70)

# 결과를 위젯으로 반환 (downstream job에서 사용 가능)
dbutils.notebook.exit(json.dumps(result))
