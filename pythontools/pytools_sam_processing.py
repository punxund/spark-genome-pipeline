#!/usr/bin/env python3
"""
Python 네이티브 도구들을 사용한 SAM 처리 모듈
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import logging
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
import tempfile
import shutil
import os
import json

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config_pytools import PyToolsConfig

logger = logging.getLogger(__name__)

def run_python_samtools_udf(sample_id: str, sam_file: str) -> dict:
    """
    Python 네이티브 samtools를 실행하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        sam_file: SAM 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    try:
        # SAM 파일 읽기 및 처리
        sam_records = []
        header_lines = []
        
        with open(sam_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('@'):
                    header_lines.append(line)
                else:
                    sam_records.append(line)
        
        logger.info(f"SAM 파일 읽기 완료: {len(sam_records)} 개의 레코드")
        
        # 품질 필터링 (MAPQ >= 20)
        config = PyToolsConfig.SAM_PROCESSING_CONFIG
        filtered_records = []
        
        for record in sam_records:
            parts = record.split('\t')
            if len(parts) >= 5:
                mapq = int(parts[4])
                if mapq >= config["filter_quality"]:
                    filtered_records.append(record)
        
        logger.info(f"품질 필터링 완료: {len(filtered_records)}/{len(sam_records)} 레코드 유지")
        
        # 중복 제거 (선택적)
        if config["remove_duplicates"]:
            unique_records = list(set(filtered_records))
            logger.info(f"중복 제거 완료: {len(unique_records)}/{len(filtered_records)} 레코드 유지")
            filtered_records = unique_records
        
        # BAM 파일 생성 (실제로는 SAM 형식으로 저장)
        bam_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_filtered.bam"
        with open(bam_file, 'w') as f:
            # 헤더 작성
            for header in header_lines:
                f.write(header + '\n')
            
            # 필터링된 레코드 작성
            for record in filtered_records:
                f.write(record + '\n')
        
        # 매핑 통계 계산
        total_reads = len(sam_records)
        mapped_reads = len([r for r in sam_records if not r.split('\t')[2].startswith('*')])
        filtered_reads = len(filtered_records)
        
        stats = {
            "total_reads": total_reads,
            "mapped_reads": mapped_reads,
            "filtered_reads": filtered_reads,
            "mapping_rate": (mapped_reads / total_reads) * 100 if total_reads > 0 else 0,
            "filter_rate": (filtered_reads / total_reads) * 100 if total_reads > 0 else 0
        }
        
        # 통계 파일 저장
        stats_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_python_samtools_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Python 네이티브 samtools 처리 완료: {sample_id}")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "bam_file": str(bam_file),
            "stats_file": str(stats_file),
            "stats": stats,
            "error": None
        }
            
    except Exception as e:
        logger.error(f"Python 네이티브 samtools 처리 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "bam_file": None,
            "stats_file": None,
            "stats": None,
            "error": str(e)
        }
    finally:
        # 임시 파일 정리
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

class PyToolsSAMProcessor:
    """Python 네이티브 도구들을 사용한 SAM 처리 클래스"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.temp_files = []
    
    def process_sam_files(self, alignment_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        """
        SAM 파일들을 Python 네이티브 도구로 처리합니다.
        
        Args:
            alignment_df: 정렬 결과 DataFrame
        
        Returns:
            처리 결과 DataFrame
        """
        logger.info("Python 네이티브 SAM 파일 처리 시작")
        
        # 성공한 정렬 결과만 필터링
        successful_df = alignment_df.filter(
            col("alignment_result.status") == "success"
        )
        
        if successful_df.count() == 0:
            logger.warning("처리할 SAM 파일이 없습니다.")
            return self.spark.createDataFrame([], StructType([
                StructField("sample_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("bam_file", StringType(), True),
                StructField("stats_file", StringType(), True),
                StructField("stats", StringType(), True),
                StructField("error", StringType(), True)
            ]))
        
        # UDF 등록
        python_samtools_udf = udf(run_python_samtools_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("bam_file", StringType(), True),
            StructField("stats_file", StringType(), True),
            StructField("stats", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # Python 네이티브 samtools 실행
        result_df = successful_df.withColumn(
            "samtools_result",
            python_samtools_udf(
                col("alignment_result.sample_id"),
                col("alignment_result.sam_file")
            )
        )
        
        logger.info("Python 네이티브 SAM 파일 처리 완료")
        return result_df

def run_sam_processing(spark: SparkSession, alignment_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    Python 네이티브 SAM 처리를 실행합니다.
    
    Args:
        spark: SparkSession
        alignment_df: 정렬 결과 DataFrame
    
    Returns:
        SAM 처리 결과 DataFrame
    """
    processor = PyToolsSAMProcessor(spark)
    return processor.process_sam_files(alignment_df)
