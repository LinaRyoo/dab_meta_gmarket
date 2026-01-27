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


def append_bad_data_for_expectations(spark, catalog="baikald1ws", bad_records_count=5):
    """
    Expectation 테스트를 위한 비정상 데이터 추가 (append 모드)
    
    생성되는 비정상 데이터:
    1. expect 위반 (Warning): OPT_GD_NO IS NULL
    2. expect_or_fail 위반 (Critical): OPT_NO IS NULL → Quarantine 테이블로 이동
    """
    
    print(f"\n⚠️  Appending Bad Data for Expectation Testing...")
    print(f"   Bad records count: {bad_records_count}")
    
    table_name = f"{catalog}.sdp_poc.item_goods_option_rdb_raw"
    
    # 기존 테이블이 있으면 스키마를 읽어옴
    try:
        existing_df = spark.table(table_name).limit(0)
        schema = existing_df.schema
        print(f"   ℹ️  Using existing table schema from {table_name}")
        print(f"   📋 Schema has {len(schema.fields)} fields")
    except Exception as e:
        print(f"\n   ❌ Table {table_name} not found!")
        print(f"   📝 Please run one of these commands first:")
        print(f"      - databricks bundle run create_mock_data --target dev")
        print(f"      - python3 scripts/create_mock_data.py --catalog {catalog}")
        raise Exception(f"Prerequisite failed: Table {table_name} must exist before appending bad data.")
    
    bad_data = []
    base_date = datetime.now()
    
    for idx in range(bad_records_count):
        # 50%는 expect 위반 (OPT_GD_NO NULL), 50%는 expect_or_fail 위반 (OPT_NO NULL)
        if idx % 2 == 0:
            # Type 1: expect 위반 - OPT_GD_NO IS NULL (Warning)
            record_type = "EXPECT_VIOLATION"
            opt_no = 9990000 + idx
            opt_gd_no = None  # 👈 OPT_GD_NO IS NULL
        else:
            # Type 2: expect_or_fail 위반 - OPT_NO IS NULL (Critical, Quarantine로)
            record_type = "EXPECT_OR_FAIL_VIOLATION"
            opt_no = None  # 👈 OPT_NO IS NULL
            opt_gd_no = 9995000 + idx
        
        record = {
            # 기본 상품 옵션 정보
            "OPT_NO": opt_no,
            "OPT_GD_NO": opt_gd_no,
            "INFO_TYPE": "99",
            "DISP_TYPE": "X",
            "OPT_NM": f"BAD_DATA_{idx}_{record_type}",
            "OPT_VALUE": f"비정상데이터_{idx}",
            "SORT_ORDER": 999,
            "OPT_PRICE": -1,  # 비정상 가격
            "INVENTORY_CNT": -1,  # 비정상 재고
            "VERSION_CHG_DT": base_date,
            "REG_ID": "test_bad_data",
            "REG_DT": base_date - timedelta(days=1),
            "CHG_ID": "test_bad_data",
            "CHG_DT": base_date,
            "FTBL": "TEST",
            "FKEY": "BAD",
            "FTYPE": "TEST",
            "CHANGED": "Y",
            "OPT_STAT": "99",
            "OPT_NM_SORT_ORDER": 999,
            "OPT_VALUE_2": f"BAD_{idx}",
            "USE_YN": "Y",
            "REP_IMAGE_URL": None,
            "OPT_MASTER_SEQ": 999,
            "SELLER_MANAGE_VALUE": f"BAD_SKU_{idx}",
            "SKU_MATCHING_VER_NO": 1,
            "ENG_OPT_NM": f"BadData_{idx}",
            "ENG_OPT_VALUE": f"Bad_{idx}",
            "ENG_OPT_VALUE_2": None,
            "SINGLE_OPT_SEQ": 999,
            "STOCK_ID": None,
            
            # CDC 정보
            "SYNC_ID": 99990000 + idx,
            "S_OPT_NO": opt_no if opt_no else (9995000 + idx),
            "OPERATION_TYPE": "I",
            "INS_DATE": base_date,
            "INS_OPRT": "bad_data_test",
            
            # 메타데이터 컬럼
            "ingest_dt": base_date.replace(hour=0, minute=0, second=0, microsecond=0),
            "_source_system": "test_system",
            "_ingestion_timestamp": base_date,
            "_dataflow_id": "item_goods_option"
        }
        
        bad_data.append(record)
    
    # DataFrame 생성 (기존 테이블 스키마 사용)
    bad_df = spark.createDataFrame(bad_data, schema=schema)
    
    # Raw Bronze 테이블에 APPEND
    bad_df.write.mode("append").partitionBy("ingest_dt").saveAsTable(table_name)
    
    print(f"✅ Bad data appended: {bad_df.count()} record(s)")
    print(f"   Table: {table_name}")
    
    # 통계 출력
    print("\n⚠️  Bad Data Statistics:")
    print("   Type 1 (expect 위반): OPT_GD_NO IS NULL → Warning, 데이터는 통과")
    print("   Type 2 (expect_or_fail 위반): OPT_NO IS NULL → Critical, Quarantine로 이동")
    bad_df.groupBy(
        F.when(F.col("OPT_NO").isNull(), "OPT_NO_NULL (Quarantine)")
         .when(F.col("OPT_GD_NO").isNull(), "OPT_GD_NO_NULL (Warning)")
         .otherwise("Normal")
         .alias("violation_type")
    ).count().show(truncate=False)
    
    return bad_df


def main():
    parser = argparse.ArgumentParser(description="Create mock data for DLT pipeline testing")
    parser.add_argument("--catalog", default="baikald1ws", help="Unity Catalog name")
    parser.add_argument("--days", type=int, default=7, help="Number of days of data")
    parser.add_argument("--records-per-day", type=int, default=10, help="Records per day")
    parser.add_argument("--skip-metadata", action="store_true", help="Skip metadata table creation")
    parser.add_argument("--skip-raw-data", action="store_true", help="Skip raw data creation")
    parser.add_argument("--append-bad-data", action="store_true", help="Append bad data for expectation testing")
    parser.add_argument("--bad-records", type=int, default=10, help="Number of bad records to append")
    
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
    
    # Bad Data 추가 (선택사항)
    if args.append_bad_data:
        append_bad_data_for_expectations(
            spark,
            args.catalog,
            args.bad_records
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
    
    if args.append_bad_data:
        print("\n3. Verify bad data (expectation violations):")
        print(f"   SELECT * FROM {args.catalog}.sdp_poc.item_goods_option_rdb_raw WHERE OPT_NM LIKE 'BAD_DATA%';")
        print(f"   -- OPT_NO IS NULL 레코드는 Quarantine 테이블로 이동됩니다")
        print(f"   -- OPT_GD_NO IS NULL 레코드는 Warning으로 기록되지만 통과됩니다")
    
    print("\n4. Run DLT pipeline:")
    print("   databricks bundle run dlt_item_goods_option --target dev")
    
    if args.append_bad_data:
        print("\n5. Check quarantine table (expect_or_fail violations):")
        print(f"   SELECT * FROM {args.catalog}.sdp_poc.item_goods_option_silver_quarantine;")
        print("\n6. Check event log (expect violations):")
        print(f"   SELECT * FROM event_log('{args.catalog}.sdp_poc.item_goods_option_silver')")
        print(f"   WHERE details:flow_progress:data_quality:expectations IS NOT NULL;")
    
    print("="*70)


if __name__ == "__main__":
    main()
