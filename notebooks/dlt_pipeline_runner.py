# Databricks notebook source
# MAGIC %pip install dlt-meta
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC # DLT-META Pipeline Runner
# MAGIC 
# MAGIC 메타데이터 기반 Bronze/Silver 파이프라인 실행
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - `layer`: "bronze", "silver", 또는 "bronze_silver"
# MAGIC - `bronze.dataflowspecTable`: Bronze 메타데이터 테이블
# MAGIC - `silver.dataflowspecTable`: Silver 메타데이터 테이블

# COMMAND ----------
# Layer 설정 가져오기
layer = spark.conf.get("layer", None)
if not layer:
    raise Exception("'layer' configuration이 설정되지 않았습니다.")

print("="*70)
print(f"🚀 DLT-META Pipeline Runner - Layer: {layer}")
print("="*70)

# COMMAND ----------
# DLT-META 파이프라인 실행 (공식 demo 패턴)
from src.dataflow_pipeline import DataflowPipeline

DataflowPipeline.invoke_dlt_pipeline(spark, layer)

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------
print("\n" + "="*70)
print("✅ DLT-META Pipeline Registration Complete!")
print("="*70)
print(f"Layer: {layer}")
print("Pipeline will execute according to DLT schedule.")
print("="*70)
