#!/usr/bin/env python3
"""
Hybrid SAM 처리 모듈 (pysam 활용)
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

from config_hybrid import HybridConfig as PyToolsConfig

logger = logging.getLogger(__name__)

def run_pysam_processing_udf(sample_id: str, sam_file: str) -> dict:
    """
    pysam을 사용하여 SAM/BAM 파일을 처리하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        sam_file: SAM 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    try:
        import pysam
        
        config = PyToolsConfig.SAM_PROCESSING_CONFIG
        
        # SAM 파일을 pysam으로 읽기
        logger.info(f"pysam으로 SAM 파일 읽기 시작: {sample_id}")
        
        # SAM 파일을 BAM으로 변환 (pysam은 BAM 형식을 더 효율적으로 처리)
        bam_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_filtered.bam"
        
        # SAM 파일을 pysam으로 읽어서 처리
        with pysam.AlignmentFile(sam_file, "r") as sam_in:
            # 헤더 정보 가져오기
            header = sam_in.header.copy()
            
            # 필터링된 레코드들을 저장할 리스트
            filtered_records = []
            total_reads = 0
            mapped_reads = 0
            
            # 각 레코드 처리
            for record in sam_in:
                total_reads += 1
                
                # 매핑된 리드인지 확인
                if not record.is_unmapped:
                    mapped_reads += 1
                    
                    # 품질 필터링 (MAPQ >= 20)
                    if record.mapping_quality >= config["filter_quality"]:
                        filtered_records.append(record)
            
            # 레코드를 위치별로 정렬 (BAM 인덱싱을 위해)
            filtered_records.sort(key=lambda x: (x.reference_name, x.reference_start))
            
            # 중복 제거 (선택적)
            if config["remove_duplicates"]:
                # 중복 제거를 위한 딕셔너리
                unique_records = {}
                for record in filtered_records:
                    # 중복 판별을 위한 키 생성 (위치 + 시퀀스)
                    key = (record.reference_name, record.reference_start, record.query_sequence)
                    if key not in unique_records:
                        unique_records[key] = record
                
                filtered_records = list(unique_records.values())
                logger.info(f"중복 제거 완료: {len(filtered_records)}/{len(filtered_records)} 레코드 유지")
            
            # 필터링된 BAM 파일 생성
            with pysam.AlignmentFile(str(bam_file), "wb", header=header) as bam_out:
                for record in filtered_records:
                    bam_out.write(record)
            
            # BAM 파일 인덱스 생성 (정렬되지 않은 파일은 인덱스 생성 불가)
            try:
                pysam.index(str(bam_file))
                logger.info(f"BAM 인덱스 생성 완료: {bam_file}")
            except Exception as e:
                logger.warning(f"BAM 인덱스 생성 실패 (정렬되지 않은 파일): {e}")
                # 인덱스 생성 실패해도 계속 진행
        
        # 통계 계산
        filtered_reads = len(filtered_records)
        stats = {
            "total_reads": total_reads,
            "mapped_reads": mapped_reads,
            "filtered_reads": filtered_reads,
            "mapping_rate": (mapped_reads / total_reads) * 100 if total_reads > 0 else 0,
            "filter_rate": (filtered_reads / total_reads) * 100 if total_reads > 0 else 0,
            "average_mapping_quality": sum(r.mapping_quality for r in filtered_records) / len(filtered_records) if filtered_records else 0
        }
        
        # 통계 파일 저장
        stats_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_pysam_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"pysam SAM 처리 완료: {sample_id}")
        logger.info(f"  - 총 리드: {total_reads}")
        logger.info(f"  - 매핑된 리드: {mapped_reads}")
        logger.info(f"  - 필터링된 리드: {filtered_reads}")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "bam_file": str(bam_file),
            "stats_file": str(stats_file),
            "stats": stats,
            "error": None
        }
            
    except ImportError:
        logger.error("pysam 라이브러리가 설치되지 않았습니다.")
        return {
            "sample_id": sample_id,
            "status": "error",
            "bam_file": None,
            "stats_file": None,
            "stats": None,
            "error": "pysam 라이브러리가 설치되지 않았습니다."
        }
    except Exception as e:
        logger.error(f"pysam SAM 처리 중 예외 발생: {sample_id} - {str(e)}")
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

class HybridSAMProcessor:
    """pysam을 사용한 SAM 처리 클래스"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.temp_files = []
    
    def process_sam_files(self, alignment_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        """
        SAM 파일들을 pysam으로 처리합니다.
        
        Args:
            alignment_df: 정렬 결과 DataFrame
        
        Returns:
            처리 결과 DataFrame
        """
        logger.info("pysam SAM 파일 처리 시작")
        
        # 성공한 정렬 결과만 필터링
        successful_df = alignment_df.filter(
            col("bwa_result.status") == "success"
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
        pysam_processing_udf = udf(run_pysam_processing_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("bam_file", StringType(), True),
            StructField("stats_file", StringType(), True),
            StructField("stats", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # pysam SAM 처리 실행
        result_df = successful_df.withColumn(
            "samtools_result",
            pysam_processing_udf(
                col("bwa_result.sample_id"),
                col("bwa_result.sam_file")
            )
        )
        
        logger.info("pysam SAM 파일 처리 완료")
        return result_df

def run_sam_processing(spark: SparkSession, alignment_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    pysam을 사용한 SAM 처리를 실행합니다.
    
    Args:
        spark: SparkSession
        alignment_df: 정렬 결과 DataFrame
    
    Returns:
        SAM 처리 결과 DataFrame
    """
    processor = HybridSAMProcessor(spark)
    return processor.process_sam_files(alignment_df)
