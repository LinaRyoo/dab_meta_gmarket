"""
통합 메타데이터 프로세서 (SSOT)

이 스크립트는 unified_pipeline_metadata.yml을 읽어서:
1. RDB Ingestion Job 설정 생성
2. dlt-meta Onboarding JSON 생성
3. Databricks Asset Bundles (DABs) 리소스 생성
4. 메타데이터 Delta 테이블 저장

Usage:
    python unified_metadata_processor.py \
        --metadata-file metadata/unified_pipeline_metadata.yml \
        --output-dir generated/ \
        --environment dev
"""

import yaml
import json
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import argparse


class UnifiedMetadataProcessor:
    """통합 메타데이터를 처리하여 각종 설정 파일을 생성하는 프로세서"""
    
    def __init__(self, metadata_file: str, output_dir: str, environment: str = "dev"):
        self.metadata_file = Path(metadata_file)
        self.output_dir = Path(output_dir)
        self.environment = environment
        self.metadata = self._load_metadata()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_metadata(self) -> Dict:
        """통합 메타데이터 YAML 로드"""
        with open(self.metadata_file, 'r', encoding='utf-8') as f:
            metadata = yaml.safe_load(f)
        print(f"✅ Loaded metadata from: {self.metadata_file}")
        return metadata
    
    def _apply_environment_overrides(self, dataflow: Dict) -> Dict:
        """환경별 설정 오버라이드 적용"""
        if "environments" in self.metadata and self.environment in self.metadata["environments"]:
            env_config = self.metadata["environments"][self.environment]
            
            # Catalog 오버라이드
            if "catalog" in env_config:
                for stage in ["raw_bronze", "bronze", "silver"]:
                    if stage in dataflow:
                        dataflow[stage]["catalog"] = env_config["catalog"]
            
            # Source connection 오버라이드
            if "source" in env_config and "connection" in env_config["source"]:
                for key, value in env_config["source"]["connection"].items():
                    dataflow["source"]["connection"][key] = value
            
            # Schedule 오버라이드
            if "schedule" in env_config:
                dataflow["schedule"].update(env_config["schedule"])
        
        return dataflow
    
    def process_all(self):
        """모든 데이터플로우 처리"""
        print("\n" + "="*70)
        print(f"🚀 Processing Unified Metadata - Environment: {self.environment}")
        print("="*70 + "\n")
        
        for dataflow in self.metadata["dataflows"]:
            if not dataflow.get("enabled", True):
                print(f"⏭️  Skipping disabled dataflow: {dataflow['dataflow_id']}")
                continue
            
            # 환경별 설정 적용
            dataflow = self._apply_environment_overrides(dataflow)
            
            print(f"\n📦 Processing dataflow: {dataflow['dataflow_id']}")
            print(f"   Description: {dataflow['description']}")
            
            # 1. RDB Ingestion 설정 생성
            self.generate_rdb_ingestion_config(dataflow)
            
            # 2. dlt-meta Onboarding JSON 생성
            self.generate_dltmeta_onboarding(dataflow)
            
            # 3. DABs 리소스 생성
            self.generate_dabs_resources(dataflow)
            
            # 4. 메타데이터 테이블 DDL 생성
            self.generate_metadata_table_ddl(dataflow)
        
        print("\n" + "="*70)
        print("✅ All configurations generated successfully!")
        print(f"📁 Output directory: {self.output_dir.absolute()}")
        print("="*70 + "\n")
    
    def generate_rdb_ingestion_config(self, dataflow: Dict):
        """RDB Ingestion Job 설정 생성"""
        dataflow_id = dataflow["dataflow_id"]
        source = dataflow["source"]
        raw_bronze = dataflow["raw_bronze"]
        
        config = {
            "job_name": f"rdb_ingestion_{dataflow_id}",
            "dataflow_id": dataflow_id,
            "description": dataflow["description"],
            "schedule": dataflow.get("schedule", {}),
            
            "source": {
                "type": source["type"],
                "connection": source["connection"],
                "extraction": {
                    "prepare_query": source["extraction"]["prepare_query"].strip(),
                    "main_query": source["extraction"]["main_query"].strip(),
                    "read_options": source["extraction"].get("read_options", {}),
                    "parameters": source["extraction"].get("parameters", {})
                }
            },
            
            "target": {
                "catalog": raw_bronze["catalog"],
                "schema": raw_bronze["schema"],
                "table": raw_bronze["table"],
                "format": raw_bronze["format"],
                "write_mode": raw_bronze["write_mode"],
                "partition_columns": raw_bronze.get("partition_columns", []),
                "table_properties": raw_bronze.get("table_properties", {}),
                "metadata_columns": raw_bronze.get("metadata_columns", {})
            }
        }
        
        output_file = self.output_dir / f"rdb_ingestion_{dataflow_id}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Generated RDB ingestion config: {output_file.name}")
        return config
    
    def generate_dltmeta_onboarding(self, dataflow: Dict):
        """dlt-meta Onboarding JSON 생성"""
        dataflow_id = dataflow["dataflow_id"]
        raw_bronze = dataflow["raw_bronze"]
        bronze = dataflow["bronze"]
        silver = dataflow["silver"]
        
        onboarding = [
            {
                "data_flow_id": dataflow_id,
                "data_flow_group": dataflow["dataflow_group"],
                "source_system": dataflow["source"]["connection"]["connection_name"],
                "source_format": bronze["source_format"],
                
                # Source details (raw_bronze 참조)
                "source_details": {
                    "source_catalog": raw_bronze["catalog"],
                    "source_database": raw_bronze["schema"],
                    "source_table": raw_bronze["table"]
                },
                
                # Bronze 설정
                "bronze_catalog_dev": bronze["catalog"],
                "bronze_database_dev": bronze["schema"],
                "bronze_table": bronze["table"],
                "bronze_table_comment": bronze.get("table_comment", ""),
                "bronze_reader_options": bronze.get("reader_options", {}),
                "bronze_partition_columns": ",".join(bronze.get("partition_columns", [])),
                "bronze_table_properties": bronze.get("table_properties", {}),
                "bronze_cluster_by": bronze.get("cluster_by", []),
                
                # Silver 설정
                "silver_catalog_dev": silver["catalog"],
                "silver_database_dev": silver["schema"],
                "silver_table": silver["table"],
                "silver_table_comment": silver.get("table_comment", ""),
                "silver_cdc_apply_changes": silver.get("cdc_apply_changes", {}),
                "silver_table_properties": silver.get("table_properties", {}),
                "silver_cluster_by": silver.get("cluster_by", []),
                
                # Silver 변환 (별도 파일로 저장)
                "silver_transformation_json_dev": f"generated/transformations_{dataflow_id}.json"
            }
        ]
        
        # Onboarding JSON 저장
        output_file = self.output_dir / f"onboarding_{dataflow_id}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(onboarding, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Generated dlt-meta onboarding: {output_file.name}")
        
        # Silver 변환 JSON 저장
        transformations = [{
            "target_table": silver["table"],
            "select_exp": silver["transformations"].get("select_expressions", []),
            "where_clause": silver["transformations"].get("where_clauses", [])
        }]
        
        transform_file = self.output_dir / f"transformations_{dataflow_id}.json"
        with open(transform_file, 'w', encoding='utf-8') as f:
            json.dump(transformations, f, indent=2, ensure_ascii=False)
        
        print(f"   ✅ Generated silver transformations: {transform_file.name}")
        
        return onboarding
    
    def generate_dabs_resources(self, dataflow: Dict):
        """Databricks Asset Bundles 리소스 생성"""
        dataflow_id = dataflow["dataflow_id"]
        
        # Job 리소스 (RDB Ingestion)
        rdb_job = {
            "name": f"rdb_ingestion_{dataflow_id}",
            "tasks": [
                {
                    "task_key": "extract_from_rdb",
                    "notebook_task": {
                        "notebook_path": "./notebooks/rdb_ingestion_runner",
                        "base_parameters": {
                            "config_file": f"generated/rdb_ingestion_{dataflow_id}.json",
                            "environment": self.environment
                        }
                    },
                    "new_cluster": {
                        "spark_version": "14.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 2,
                        "spark_conf": {
                            "spark.databricks.delta.optimizeWrite.enabled": "true",
                            "spark.databricks.delta.autoCompact.enabled": "true"
                        }
                    },
                    "libraries": [
                        {
                            "maven": {
                                "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.4.0.jre11"
                            }
                        }
                    ]
                }
            ]
        }
        
        if dataflow.get("schedule", {}).get("enabled", False):
            rdb_job["schedule"] = {
                "quartz_cron_expression": dataflow["schedule"]["cron"],
                "timezone_id": dataflow["schedule"]["timezone"]
            }
        
        # DLT Pipeline 리소스
        dlt_pipeline = {
            "name": f"dlt_{dataflow_id}",
            "catalog": dataflow["bronze"]["catalog"],
            "target": dataflow["bronze"]["schema"],
            "libraries": [
                {
                    "notebook": {
                        "path": "./notebooks/dlt_pipeline_runner"
                    }
                },
                {
                    "pypi": {
                        "package": "dlt-meta>=0.0.10"
                    }
                }
            ],
            "configuration": {
                "layer": "bronze_silver",
                "bronze.group": dataflow["dataflow_group"],
                "bronze.dataflowspecTable": f"{dataflow['bronze']['catalog']}.metadata.bronze_dataflowspec",
                "silver.group": dataflow["dataflow_group"],
                "silver.dataflowspecTable": f"{dataflow['silver']['catalog']}.metadata.silver_dataflowspec"
            },
            "clusters": [
                {
                    "label": "default",
                    "num_workers": 2
                }
            ],
            "continuous": False,
            "development": self.environment == "dev"
        }
        
        # DABs YAML 생성
        dabs_resources = {
            "resources": {
                "jobs": {
                    f"rdb_ingestion_{dataflow_id}": rdb_job
                },
                "pipelines": {
                    f"dlt_{dataflow_id}": dlt_pipeline
                }
            }
        }
        
        output_file = self.output_dir / f"dabs_{dataflow_id}.yml"
        with open(output_file, 'w', encoding='utf-8') as f:
            yaml.dump(dabs_resources, f, default_flow_style=False, sort_keys=False)
        
        print(f"   ✅ Generated DABs resources: {output_file.name}")
        
        return dabs_resources
    
    def generate_metadata_table_ddl(self, dataflow: Dict):
        """메타데이터 추적 테이블 DDL 생성"""
        dataflow_id = dataflow["dataflow_id"]
        catalog = dataflow["bronze"]["catalog"]
        
        ddl = f"""-- ============================================================================
-- 메타데이터 추적 테이블 DDL
-- Dataflow: {dataflow_id}
-- Generated: {datetime.now().isoformat()}
-- ============================================================================

-- 메타데이터 스키마 생성
CREATE SCHEMA IF NOT EXISTS {catalog}.metadata
COMMENT 'Pipeline metadata and lineage tracking';

-- 파이프라인 실행 이력 테이블
CREATE TABLE IF NOT EXISTS {catalog}.metadata.pipeline_execution_history (
  execution_id STRING NOT NULL COMMENT 'Unique execution identifier',
  dataflow_id STRING NOT NULL COMMENT 'Dataflow identifier',
  dataflow_group STRING NOT NULL COMMENT 'Dataflow group',
  stage STRING NOT NULL COMMENT 'Pipeline stage: rdb_ingestion, bronze, silver',
  status STRING NOT NULL COMMENT 'Status: running, success, failed',
  start_time TIMESTAMP NOT NULL COMMENT 'Execution start time',
  end_time TIMESTAMP COMMENT 'Execution end time',
  duration_seconds DOUBLE COMMENT 'Execution duration in seconds',
  rows_processed BIGINT COMMENT 'Number of rows processed',
  error_message STRING COMMENT 'Error message if failed',
  metadata MAP<STRING, STRING> COMMENT 'Additional metadata',
  created_at TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (DATE(start_time))
COMMENT 'Pipeline execution history and monitoring';

-- 데이터 계보 (Lineage) 테이블
CREATE TABLE IF NOT EXISTS {catalog}.metadata.data_lineage (
  lineage_id STRING NOT NULL COMMENT 'Unique lineage identifier',
  dataflow_id STRING NOT NULL COMMENT 'Dataflow identifier',
  source_system STRING COMMENT 'Source system name',
  source_table STRING COMMENT 'Fully qualified source table name',
  target_table STRING NOT NULL COMMENT 'Fully qualified target table name',
  transformation_logic STRING COMMENT 'Transformation logic applied',
  execution_id STRING COMMENT 'Reference to pipeline_execution_history',
  created_at TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Data lineage tracking';

-- 메타데이터 버전 관리 테이블
CREATE TABLE IF NOT EXISTS {catalog}.metadata.metadata_versions (
  version_id STRING NOT NULL COMMENT 'Version identifier',
  dataflow_id STRING NOT NULL COMMENT 'Dataflow identifier',
  metadata_content STRING NOT NULL COMMENT 'Full metadata YAML/JSON content',
  change_description STRING COMMENT 'Description of changes',
  created_by STRING NOT NULL COMMENT 'User who created this version',
  created_at TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  is_active BOOLEAN DEFAULT true COMMENT 'Is this the active version'
)
USING DELTA
COMMENT 'Metadata version control';

-- 샘플 데이터 삽입 (현재 메타데이터)
INSERT INTO {catalog}.metadata.metadata_versions (
  version_id,
  dataflow_id,
  metadata_content,
  change_description,
  created_by,
  is_active
) VALUES (
  uuid(),
  '{dataflow_id}',
  '--- Metadata would be stored here ---',
  'Initial metadata version',
  current_user(),
  true
);

-- 뷰 생성: 최신 실행 상태
CREATE OR REPLACE VIEW {catalog}.metadata.v_latest_pipeline_status AS
SELECT 
  dataflow_id,
  stage,
  status,
  start_time,
  end_time,
  duration_seconds,
  rows_processed,
  error_message
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY dataflow_id, stage 
      ORDER BY start_time DESC
    ) as rn
  FROM {catalog}.metadata.pipeline_execution_history
)
WHERE rn = 1;

-- 뷰 생성: 전체 데이터 플로우
CREATE OR REPLACE VIEW {catalog}.metadata.v_complete_dataflow AS
SELECT 
  l.dataflow_id,
  l.source_system,
  l.source_table,
  l.target_table,
  p.status,
  p.start_time,
  p.end_time,
  p.rows_processed
FROM {catalog}.metadata.data_lineage l
LEFT JOIN {catalog}.metadata.v_latest_pipeline_status p 
  ON l.dataflow_id = p.dataflow_id
ORDER BY l.dataflow_id, l.created_at;
"""
        
        output_file = self.output_dir / f"metadata_ddl_{dataflow_id}.sql"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(ddl)
        
        print(f"   ✅ Generated metadata DDL: {output_file.name}")
        
        return ddl


def main():
    parser = argparse.ArgumentParser(
        description="Process unified pipeline metadata (SSOT)"
    )
    parser.add_argument(
        "--metadata-file",
        required=True,
        help="Path to unified metadata YAML file"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for generated files"
    )
    parser.add_argument(
        "--environment",
        default="dev",
        choices=["dev", "prod"],
        help="Target environment"
    )
    
    args = parser.parse_args()
    
    processor = UnifiedMetadataProcessor(
        metadata_file=args.metadata_file,
        output_dir=args.output_dir,
        environment=args.environment
    )
    
    processor.process_all()


if __name__ == "__main__":
    main()
