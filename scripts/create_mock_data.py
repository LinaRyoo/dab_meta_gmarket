#!/usr/bin/env python3
"""
Mock 데이터 생성 스크립트

RDB 연결 없이 DLT 파이프라인을 테스트하기 위한 mock 데이터를 생성합니다.

- 메타데이터 테이블 (bronze_dataflowspec, silver_dataflowspec)
- Raw Bronze 테이블 (item_goods_option_rdb_raw)
"""

import argparse
import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
import uuid

# Spark Session은 Databricks에서 자동 제공됨 (spark 변수)

def create_schemas(spark, catalog="baikald1ws"):
    """필요한 스키마 생성"""
    
    print("\n🔧 Creating Schemas...")
    
    # Catalog 존재 확인 (Unity Catalog)
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    if catalog not in catalogs:
        print(f"   ⚠️  Catalog '{catalog}' not found. Please create it first.")
        raise Exception(f"Catalog '{catalog}' does not exist")
    
    # metadata 스키마 생성
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.metadata")
    print(f"   ✅ Schema created/verified: {catalog}.metadata")
    
    # sdp_poc 스키마 생성
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.sdp_poc")
    print(f"   ✅ Schema created/verified: {catalog}.sdp_poc")


def create_bronze_dataflowspec(spark, catalog="baikald1ws"):
    """Bronze dataflowspec 메타데이터 삽입"""
    
    print("\n📊 Creating Bronze Dataflowspec...")
    
    # dlt-meta가 읽는 bronze dataflowspec 테이블에 레코드 삽입 (camelCase 컬럼명)
    data = [{
        "dataFlowId": "item_goods_option",
        "dataFlowGroup": "item_group",
        "sourceFormat": "delta",
        "sourceDetails": {
            "source_catalog": catalog,
            "source_database": "sdp_poc",
            "source_table": "item_goods_option_rdb_raw"
        },
        "readerConfigOptions": {},
        "targetFormat": "delta",  # dlt-meta 표준 필드
        "targetDetails": {
            "catalog": catalog,
            "database": "sdp_poc",
            "table": "item_goods_option_bronze",
            "comment": "상품 옵션 정보 Bronze 테이블"
        },
        "tableProperties": {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true"
        },
        "schema": None,  # Bronze schema (optional)
        "partitionColumns": [],  # Liquid Clustering 사용 시 빈 리스트
        "cdcApplyChanges": None,  # Bronze에서는 일반적으로 None
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": {},
        "quarantineTableProperties": {},
        "appendFlows": None,
        "appendFlowsSchemas": {},
        "clusterBy": ["OPT_NO"],
        "sinks": None,
        "version": "v1",  # 메타데이터 버전
        "createDate": datetime.now(),
        "createdBy": "dlt-meta-admin",
        "updateDate": datetime.now(),
        "updatedBy": "dlt-meta-admin"
    }]
    
    df = spark.createDataFrame(data, schema=StructType([
        StructField("dataFlowId", StringType(), False),
        StructField("dataFlowGroup", StringType(), False),
        StructField("sourceFormat", StringType(), False),
        StructField("sourceDetails", MapType(StringType(), StringType()), False),
        StructField("readerConfigOptions", MapType(StringType(), StringType()), True),
        StructField("targetFormat", StringType(), False),
        StructField("targetDetails", MapType(StringType(), StringType()), False),
        StructField("tableProperties", MapType(StringType(), StringType()), True),
        StructField("schema", StringType(), True),
        StructField("partitionColumns", ArrayType(StringType()), True),
        StructField("cdcApplyChanges", StringType(), True),
        StructField("applyChangesFromSnapshot", StringType(), True),
        StructField("dataQualityExpectations", StringType(), True),
        StructField("quarantineTargetDetails", MapType(StringType(), StringType()), True),
        StructField("quarantineTableProperties", MapType(StringType(), StringType()), True),
        StructField("appendFlows", StringType(), True),
        StructField("appendFlowsSchemas", MapType(StringType(), StringType()), True),
        StructField("clusterBy", ArrayType(StringType()), True),
        StructField("sinks", StringType(), True),
        StructField("version", StringType(), True),
        StructField("createDate", TimestampType(), False),
        StructField("createdBy", StringType(), False),
        StructField("updateDate", TimestampType(), False),
        StructField("updatedBy", StringType(), False)
    ]))
    
    # 테이블 생성 또는 덮어쓰기
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.metadata.bronze_dataflowspec")
    
    print(f"✅ Bronze dataflowspec created: {df.count()} record(s)")


def create_silver_dataflowspec(spark, catalog="baikald1ws"):
    """Silver dataflowspec 메타데이터 삽입"""
    
    print("\n📊 Creating Silver Dataflowspec...")
    
    # Silver transformation JSON - dlt-meta 형식으로 구조화
    transformation_json = {
        "select_exp": [
            # 비즈니스 컬럼들
            "OPT_NO", "OPT_GD_NO", "INFO_TYPE", "DISP_TYPE", "OPT_NM",
            "OPT_VALUE", "SORT_ORDER", "OPT_PRICE", "INVENTORY_CNT",
            "VERSION_CHG_DT", "REG_ID", "REG_DT", "CHG_ID", "CHG_DT as CHG_DT_ORIG",
            "FTBL", "FKEY", "FTYPE", "CHANGED", "OPT_STAT",
            "OPT_NM_SORT_ORDER", "OPT_VALUE_2", "USE_YN", "REP_IMAGE_URL",
            "OPT_MASTER_SEQ", "SELLER_MANAGE_VALUE", "SKU_MATCHING_VER_NO",
            "ENG_OPT_NM", "ENG_OPT_VALUE", "ENG_OPT_VALUE_2",
            "SINGLE_OPT_SEQ", "STOCK_ID",
            # CDC 컬럼들 (원본)
            "SYNC_ID", "S_OPT_NO", "OPERATION_TYPE", "INS_DATE", "INS_OPRT",
            # 메타데이터 컬럼들 (필터링에 사용)
            "ingest_dt", "_source_system", "_ingestion_timestamp", "_dataflow_id",
            # 계산된 컬럼들
            "CASE WHEN OPERATION_TYPE = 'D' THEN INS_DATE ELSE CHG_DT_ORIG END as CHG_DT",
            "CASE WHEN OPERATION_TYPE = 'D' THEN 'Y' ELSE 'N' END as is_del"
        ],
        "where_clause": [
            "ingest_dt > date_sub(current_date(), 1)",
            "ingest_dt <= current_date()",
            "((OPERATION_TYPE IN ('I', 'U') AND opt_no IS NOT NULL) OR (OPERATION_TYPE IN ('D') AND opt_no IS NULL))"
        ]
    }
    
    data = [{
        "dataFlowId": "item_goods_option",
        "dataFlowGroup": "item_group",
        "sourceFormat": "delta",
        "sourceDetails": {
            "catalog": catalog,
            "database": "sdp_poc",
            "table": "item_goods_option_bronze"
        },
        "readerConfigOptions": {},  # dlt-meta 표준 필드
        "targetFormat": "delta",  # dlt-meta 표준 필드
        "targetDetails": {
            "catalog": catalog,
            "database": "sdp_poc",
            "table": "item_goods_option_silver",
            "comment": "상품 옵션 정보 Silver 테이블 (SCD Type 1)"
        },
        "cdcApplyChanges": json.dumps({
            "keys": ["OPT_NO"],
            "sequence_by": "SYNC_ID",
            "scd_type": "1",
            "apply_as_deletes": "is_del = 'Y'",
            "except_column_list": [
                "SYNC_ID", "S_OPT_NO", "OPERATION_TYPE", "INS_DATE",
                "INS_OPRT", "ingest_dt", "_source_system", "_ingestion_timestamp",
                "_dataflow_id", "CHG_DT_ORIG", "is_del"
            ]
        }),
        "tableProperties": {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true"
        },
        "partitionColumns": [],  # Liquid Clustering 사용 시 빈 리스트
        "clusterBy": ["OPT_NO"],
        "selectExp": transformation_json["select_exp"],
        "whereClause": transformation_json["where_clause"],
        "applyChangesFromSnapshot": None,  # Silver에서는 일반적으로 None (CDC 사용 시)
        "quarantineTargetDetails": {},
        "quarantineTableProperties": {},
        "appendFlows": None,
        "appendFlowsSchemas": {},
        "sinks": None,
        
        # Data Quality Expectations (dlt-meta 형식 - JSON String)
        "dataQualityExpectations": json.dumps({
            "expect": {
                "valid_opt_gd_no": "OPT_GD_NO IS NOT NULL"
            },
            "expect_or_fail": {
                "valid_opt_no": "OPT_NO IS NOT NULL"
            }
        }),
        "version": "v1",  # 메타데이터 버전
        "createDate": datetime.now(),
        "createdBy": "dlt-meta-admin",
        "updateDate": datetime.now(),
        "updatedBy": "dlt-meta-admin"
    }]
    
    # 명시적인 스키마 정의 (camelCase로 변경)
    schema = StructType([
        StructField("dataFlowId", StringType(), False),
        StructField("dataFlowGroup", StringType(), False),
        StructField("sourceFormat", StringType(), False),
        StructField("sourceDetails", MapType(StringType(), StringType()), False),
        StructField("readerConfigOptions", MapType(StringType(), StringType()), True),
        StructField("targetFormat", StringType(), False),
        StructField("targetDetails", MapType(StringType(), StringType()), False),
        StructField("cdcApplyChanges", StringType(), True),  # JSON String
        StructField("tableProperties", MapType(StringType(), StringType()), True),
        StructField("partitionColumns", ArrayType(StringType()), True),
        StructField("clusterBy", ArrayType(StringType()), True),
        StructField("selectExp", ArrayType(StringType()), True),
        StructField("whereClause", ArrayType(StringType()), True),
        StructField("applyChangesFromSnapshot", StringType(), True),
        StructField("quarantineTargetDetails", MapType(StringType(), StringType()), True),
        StructField("quarantineTableProperties", MapType(StringType(), StringType()), True),
        StructField("appendFlows", StringType(), True),
        StructField("appendFlowsSchemas", MapType(StringType(), StringType()), True),
        StructField("sinks", StringType(), True),
        StructField("dataQualityExpectations", StringType(), True),  # JSON String
        StructField("version", StringType(), True),
        StructField("createDate", TimestampType(), False),
        StructField("createdBy", StringType(), False),
        StructField("updateDate", TimestampType(), False),
        StructField("updatedBy", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema=schema)
    
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.metadata.silver_dataflowspec")
    
    print(f"✅ Silver dataflowspec created: {df.count()} record(s)")


def create_raw_bronze_mock_data(spark, catalog="baikald1ws", days=7, records_per_day=10):
    """Raw Bronze 테이블에 Mock 데이터 생성 (CDC 포함)"""
    
    print(f"\n📦 Creating Mock Raw Bronze Data...")
    print(f"   Days: {days}, Records per day: {records_per_day}")
    print(f"   Total records: {days * records_per_day}")
    
    mock_data = []
    base_date = datetime.now()
    
    for day_offset in range(days):
        ingest_date = base_date - timedelta(days=day_offset)
        
        for record_idx in range(records_per_day):
            opt_no = 1000000 + (day_offset * records_per_day) + record_idx
            opt_gd_no = 500000 + (day_offset * 100) + record_idx
            sync_id = opt_no + 10000000
            
            # CDC 타입 분배: 70% INSERT, 20% UPDATE, 10% DELETE
            rand = random.random()
            if rand < 0.7:
                operation_type = 'I'
                opt_no_value = opt_no  # INSERT는 OPT_NO 있음
            elif rand < 0.9:
                operation_type = 'U'
                opt_no_value = opt_no  # UPDATE도 OPT_NO 있음
            else:
                operation_type = 'D'
                opt_no_value = None  # DELETE는 OPT_NO NULL
            
            record = {
                # 기본 상품 옵션 정보
                "OPT_NO": opt_no_value,
                "OPT_GD_NO": opt_gd_no if opt_no_value else None,
                "INFO_TYPE": random.choice(["01", "02", "03"]),
                "DISP_TYPE": random.choice(["S", "C"]),
                "OPT_NM": f"옵션명_{opt_no}",
                "OPT_VALUE": f"옵션값_{record_idx}",
                "SORT_ORDER": record_idx + 1,
                "OPT_PRICE": random.randint(1000, 50000),
                "INVENTORY_CNT": random.randint(0, 100),
                "VERSION_CHG_DT": ingest_date - timedelta(hours=random.randint(1, 24)),
                "REG_ID": "system",
                "REG_DT": ingest_date - timedelta(days=30),
                "CHG_ID": "system",
                "CHG_DT": ingest_date - timedelta(hours=random.randint(1, 24)),
                "FTBL": "GOODS",
                "FKEY": str(opt_gd_no),
                "FTYPE": "OPT",
                "CHANGED": "Y" if operation_type in ['U', 'D'] else "N",
                "OPT_STAT": random.choice(["10", "20", "30"]),
                "OPT_NM_SORT_ORDER": record_idx + 1,
                "OPT_VALUE_2": f"추가값_{record_idx}",
                "USE_YN": "N" if operation_type == 'D' else "Y",
                "REP_IMAGE_URL": f"http://image.gmarket.co.kr/{opt_gd_no}.jpg",
                "OPT_MASTER_SEQ": record_idx + 1,
                "SELLER_MANAGE_VALUE": f"SKU_{opt_no}",
                "SKU_MATCHING_VER_NO": 1,
                "ENG_OPT_NM": f"Option_{opt_no}",
                "ENG_OPT_VALUE": f"Value_{record_idx}",
                "ENG_OPT_VALUE_2": f"Extra_{record_idx}",
                "SINGLE_OPT_SEQ": record_idx + 1,
                "STOCK_ID": str(opt_no),
                
                # CDC 정보
                "SYNC_ID": sync_id,
                "S_OPT_NO": opt_no,  # CDC 테이블의 OPT_NO (항상 있음)
                "OPERATION_TYPE": operation_type,
                "INS_DATE": ingest_date,
                "INS_OPRT": "batch_sync",
                
                # 메타데이터 컬럼
                "ingest_dt": ingest_date.replace(hour=0, minute=0, second=0, microsecond=0),
                "_source_system": "gdevdb02",
                "_ingestion_timestamp": ingest_date,
                "_dataflow_id": "item_goods_option"
            }
            
            mock_data.append(record)
    
    # DataFrame 생성
    df = spark.createDataFrame(mock_data)
    
    # Raw Bronze 테이블에 저장
    table_name = f"{catalog}.sdp_poc.item_goods_option_rdb_raw"
    df.write.mode("overwrite").option("overwriteSchema", "true").partitionBy("ingest_dt").saveAsTable(table_name)
    
    print(f"✅ Mock data created: {df.count()} record(s)")
    print(f"   Table: {table_name}")
    
    # 통계 출력
    print("\n📊 Data Statistics:")
    df.groupBy("OPERATION_TYPE").count().orderBy("OPERATION_TYPE").show()
    df.groupBy("ingest_dt").count().orderBy("ingest_dt", ascending=False).show()


def main():
    parser = argparse.ArgumentParser(description="Create mock data for DLT pipeline testing")
    parser.add_argument("--catalog", default="baikald1ws", help="Unity Catalog name")
    parser.add_argument("--days", type=int, default=7, help="Number of days of data")
    parser.add_argument("--records-per-day", type=int, default=10, help="Records per day")
    parser.add_argument("--skip-metadata", action="store_true", help="Skip metadata table creation")
    parser.add_argument("--skip-raw-data", action="store_true", help="Skip raw data creation")
    
    args = parser.parse_args()
    
    print("="*70)
    print("🎯 Mock Data Generator")
    print("="*70)
    print(f"Catalog: {args.catalog}")
    print(f"Days: {args.days}")
    print(f"Records per day: {args.records_per_day}")
    print(f"Total records: {args.days * args.records_per_day}")
    print("="*70)
    
    # Step 1: 스키마 생성
    create_schemas(spark, args.catalog)
    
    # Step 2: Metadata 테이블 생성
    if not args.skip_metadata:
        create_bronze_dataflowspec(spark, args.catalog)
        create_silver_dataflowspec(spark, args.catalog)
    
    # Raw Bronze 데이터 생성
    if not args.skip_raw_data:
        create_raw_bronze_mock_data(
            spark, 
            args.catalog, 
            args.days, 
            args.records_per_day
        )
    
    print("\n" + "="*70)
    print("✅ Mock Data Generation Complete!")
    print("="*70)
    print("\nNext steps:")
    print("1. Verify metadata tables:")
    print(f"   SELECT * FROM {args.catalog}.metadata.bronze_dataflowspec;")
    print(f"   SELECT * FROM {args.catalog}.metadata.silver_dataflowspec;")
    print("\n2. Verify raw data:")
    print(f"   SELECT * FROM {args.catalog}.sdp_poc.item_goods_option_rdb_raw LIMIT 10;")
    print("\n3. Run DLT pipeline:")
    print("   databricks bundle run dlt_item_goods_option --target dev")
    print("="*70)


if __name__ == "__main__":
    main()
