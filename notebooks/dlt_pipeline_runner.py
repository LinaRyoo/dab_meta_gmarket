# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Spark Declarative Pipeline Runner (dlt-meta)
# MAGIC 
# MAGIC 이 노트북은 dlt-meta를 사용하여 Bronze/Silver 파이프라인을 실행합니다.
# MAGIC 
# MAGIC **변경 사항 (2026):**
# MAGIC - Delta Live Tables (DLT) → Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC - `import dlt` → `from pyspark import pipelines as dp`
# MAGIC 
# MAGIC **설정 방법:**
# MAGIC - Pipeline 설정에서 다음 configuration 필요:
# MAGIC   - `layer`: "bronze", "silver", 또는 "bronze_silver"
# MAGIC   - `bronze.group`: 데이터플로우 그룹
# MAGIC   - `bronze.dataflowspecTable`: Bronze dataflowspec 테이블
# MAGIC   - `silver.group`: 데이터플로우 그룹 (선택)
# MAGIC   - `silver.dataflowspecTable`: Silver dataflowspec 테이블 (선택)

# COMMAND ----------
# MAGIC %md ## Import Libraries

# COMMAND ----------
# MAGIC %md ### Install dlt-meta Package

# COMMAND ----------
# MAGIC %pip install dlt-meta
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
# Lakeflow Spark Declarative Pipelines (최신)
from pyspark import pipelines as dp

import sys
from pyspark.sql import SparkSession

# COMMAND ----------
# MAGIC %md ### Import dlt-meta Libraries

# COMMAND ----------
# dlt-meta 라이브러리 import (pip install dlt-meta 필요)
# Note: dlt-meta 패키지는 src 모듈 구조를 유지하고 있습니다
try:
    from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils
    from src.dataflow_pipeline import DataflowPipeline
    from src.onboard_dataflowspec import OnboardDataflowspec
    print("✅ dlt-meta libraries imported successfully")
except ImportError as e:
    print(f"❌ dlt-meta import 실패: {e}")
    print("\n해결 방법:")
    print("1. DLT Pipeline 설정 → Libraries → PyPI에 'dlt-meta' 추가")
    print("2. 또는 위의 '%pip install dlt-meta' 셀 실행")
    raise

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------
# DLT Pipeline Configuration에서 값 가져오기
spark = SparkSession.builder.getOrCreate()

layer = spark.conf.get("layer", None)
if not layer:
    raise Exception("'layer' configuration이 설정되지 않았습니다. bronze, silver, 또는 bronze_silver 중 선택하세요.")

print("="*70)
print("DLT-META Pipeline Runner")
print("="*70)
print(f"Layer: {layer}")
print("="*70)

# COMMAND ----------
# MAGIC %md ## Bronze Layer

# COMMAND ----------
if layer in ["bronze", "bronze_silver"]:
    print("\n🥉 Processing Bronze Layer...")
    
    # Bronze dataflowspec 가져오기
    try:
        bronze_dataflow_specs = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
        print(f"✅ Found {len(bronze_dataflow_specs)} bronze dataflow(s)")
        
        # 각 Bronze dataflow 처리
        for bronze_spec in bronze_dataflow_specs:
            print(f"\n📦 Processing Bronze dataflow: {bronze_spec.dataFlowId}")
            
            # Bronze 테이블 이름
            source_details = dict(bronze_spec.sourceDetails) if bronze_spec.sourceDetails else {}
            target_details = dict(bronze_spec.targetDetails) if bronze_spec.targetDetails else {}
            
            bronze_table_name = target_details.get("table", bronze_spec.dataFlowId)
            
            # DataflowPipeline 생성 및 실행
            @dp.table(
                name=bronze_table_name,
                comment=target_details.get("comment", f"Bronze table for {bronze_spec.dataFlowId}"),
                table_properties=dict(bronze_spec.tableProperties) if bronze_spec.tableProperties else {},
                partition_cols=bronze_spec.partitionColumns if bronze_spec.partitionColumns else None,
                path=target_details.get("path") if target_details.get("path") else None
            )
            def create_bronze_table():
                """Bronze 테이블 생성"""
                bronze_pipeline = DataflowPipeline(
                    spark=spark,
                    dataflow_spec=bronze_spec,
                    view_name=f"{bronze_table_name}_view"
                )
                return bronze_pipeline.read_bronze()
            
            print(f"✅ Bronze table registered: {bronze_table_name}")
    
    except Exception as e:
        print(f"❌ Error processing Bronze layer: {str(e)}")
        raise

# COMMAND ----------
# MAGIC %md ## Silver Layer

# COMMAND ----------
if layer in ["silver", "bronze_silver"]:
    print("\n🥈 Processing Silver Layer...")
    
    # Silver dataflowspec 가져오기
    try:
        silver_dataflow_specs = DataflowSpecUtils.get_silver_dataflow_spec(spark)
        print(f"✅ Found {len(silver_dataflow_specs)} silver dataflow(s)")
        
        # 각 Silver dataflow 처리
        for silver_spec in silver_dataflow_specs:
            print(f"\n📦 Processing Silver dataflow: {silver_spec.dataFlowId}")
            
            # Silver 테이블 이름
            target_details = dict(silver_spec.targetDetails) if silver_spec.targetDetails else {}
            silver_table_name = target_details.get("table", f"{silver_spec.dataFlowId}_silver")
            
            # CDC Apply Changes가 있는 경우
            if silver_spec.cdcApplyChanges:
                print(f"   Using CDC Apply Changes (SCD Type {silver_spec.cdcApplyChanges})")
                
                # Source view 생성 (temporary_view로 변경)
                @dp.temporary_view(name=f"{silver_table_name}_source_view")
                def create_silver_source_view():
                    """Silver source view 생성"""
                    silver_pipeline = DataflowPipeline(
                        spark=spark,
                        dataflow_spec=silver_spec,
                        view_name=f"{silver_table_name}_source_view"
                    )
                    return silver_pipeline.read_silver()
                
                # Target 테이블에 CDC 적용
                silver_pipeline = DataflowPipeline(
                    spark=spark,
                    dataflow_spec=silver_spec,
                    view_name=f"{silver_table_name}_source_view"
                )
                silver_pipeline.write_silver()
                
            else:
                # 일반 Silver 테이블
                @dp.table(
                    name=silver_table_name,
                    comment=target_details.get("comment", f"Silver table for {silver_spec.dataFlowId}"),
                    table_properties=dict(silver_spec.tableProperties) if silver_spec.tableProperties else {},
                    partition_cols=silver_spec.partitionColumns if silver_spec.partitionColumns else None,
                    path=target_details.get("path") if target_details.get("path") else None
                )
                def create_silver_table():
                    """Silver 테이블 생성"""
                    silver_pipeline = DataflowPipeline(
                        spark=spark,
                        dataflow_spec=silver_spec,
                        view_name=f"{silver_table_name}_view"
                    )
                    return silver_pipeline.read_silver()
            
            print(f"✅ Silver table registered: {silver_table_name}")
    
    except Exception as e:
        print(f"❌ Error processing Silver layer: {str(e)}")
        raise

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------
print("\n" + "="*70)
print("✅ Lakeflow Spark Declarative Pipeline Configuration Complete!")
print("="*70)
print(f"Layer: {layer}")
print(f"API: Lakeflow SDP (pyspark.pipelines as dp)")
print("\nPipeline will now execute...")
print("="*70)
