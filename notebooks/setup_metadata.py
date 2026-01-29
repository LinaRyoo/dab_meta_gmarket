# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Metadata Tables

# COMMAND ----------
dbutils.widgets.text("sql_file_path", "", "SQL File Path")
sql_file_path = dbutils.widgets.get("sql_file_path")

print(f"Executing SQL file: {sql_file_path}")

# COMMAND ----------
# SQL 파일 읽기 및 실행
with open(sql_file_path, 'r', encoding='utf-8') as f:
    sql_content = f.read()

# 세미콜론으로 분리하여 각 statement 실행
statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

success = 0
failed = 0

for i, stmt in enumerate(statements, 1):
    try:
        spark.sql(stmt)
        success += 1
        print(f"[{i}/{len(statements)}] ✅")
    except Exception as e:
        failed += 1
        print(f"[{i}/{len(statements)}] ❌ {str(e)[:100]}")

print(f"\n✅ Success: {success}, ❌ Failed: {failed}")