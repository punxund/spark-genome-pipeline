#!/usr/bin/env python3
"""
4단계 커버리지 계산만 실행하는 스크립트
기존 BAM 파일을 사용하여 커버리지 계산을 수행합니다.
"""

import sys
import time
import json
from datetime import datetime
from pathlib import Path

# Spark 관련 import
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lit
from pyspark.sql.types import StructType, StructField, StringType

# 프로젝트 모듈 import
from config import Config
from utils import parse_fastq_pairs
from coverage import run_coverage_calculation
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """Spark 세션을 생성합니다."""
    spark = SparkSession.builder \
        .appName("Genome Coverage Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()
    
    logger.info(f"Spark 세션 생성: {spark.sparkContext.applicationId}")
    return spark

def create_bam_dataframe(spark: SparkSession, bam_file: Path) -> "pyspark.sql.DataFrame":
    """
    기존 BAM 파일로부터 DataFrame을 생성합니다.
    
    Args:
        spark: SparkSession
        bam_file: BAM 파일 경로
    
    Returns:
        BAM 파일 정보가 포함된 DataFrame
    """
    # BAM 파일에서 샘플 ID 추출 (파일명에서)
    sample_id = bam_file.stem.replace("_filtered", "")
    
    # BAM 파일 정보를 포함한 DataFrame 생성
    data = [(sample_id, str(bam_file))]
    schema = StructType([
        StructField("sample_id", StringType(), False),
        StructField("bam_file", StringType(), False)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # samtools_result 구조체 생성 (기존 파이프라인과 호환)
    df = df.withColumn("samtools_result", struct(
        col("sample_id").alias("sample_id"),
        col("bam_file").alias("bam_file"),
        lit("success").alias("status"),
        lit(None).alias("sam_file"),
        lit(None).alias("mapping_stats"),
        lit(None).alias("error")
    ))
    
    return df

def main():
    """메인 함수"""
    try:
        # Spark 세션 생성
        spark = create_spark_session()
        
        # 참조 인덱스 파일 확인
        reference_index = Config.REFERENCE_INDEX
        if not reference_index.exists():
            logger.error(f"참조 인덱스 파일을 찾을 수 없습니다: {reference_index}")
            return
        
        # 기존 BAM 파일 찾기
        bam_file = Config.RESULTS_DIR / "SRR30977596_filtered.bam"
        if not bam_file.exists():
            logger.error(f"BAM 파일을 찾을 수 없습니다: {bam_file}")
            logger.info("다음 위치에서 BAM 파일을 찾아보세요:")
            logger.info("  - results/spark_pipeline/SRR30977596_filtered.bam")
            logger.info("  - work/ 디렉토리의 하위 폴더들")
            return
        
        logger.info(f"BAM 파일 발견: {bam_file}")
        
        # BAM 파일로부터 DataFrame 생성
        logger.info("BAM 파일 정보 DataFrame 생성 중...")
        bam_df = create_bam_dataframe(spark, bam_file)
        
        # 4단계: 커버리지 계산 실행
        logger.info("=" * 50)
        logger.info("4단계: 커버리지 계산 (bedtools)")
        logger.info("=" * 50)
        
        step4_start = time.time()
        coverage_df = run_coverage_calculation(spark, bam_df, reference_index)
        
        step4_time = time.time() - step4_start
        
        # 결과 확인
        success_count = coverage_df.filter(col("coverage_result.status") == "success").count()
        total_count = coverage_df.count()
        
        logger.info(f"커버리지 계산 완료: {success_count}/{total_count} 성공 ({step4_time:.2f}초)")
        
        # 결과 요약
        logger.info("\n" + "=" * 80)
        logger.info("커버리지 계산 완료")
        logger.info("=" * 80)
        
        if success_count > 0:
            logger.info("생성된 파일들:")
            coverage_bed = Config.RESULTS_DIR / "SRR30977596_coverage.bed"
            if coverage_bed.exists():
                size_mb = coverage_bed.stat().st_size / (1024 * 1024)
                logger.info(f"  - Coverage BED: {coverage_bed} ({size_mb:.2f}MB)")
            
            bigwig_file = Config.RESULTS_DIR / "SRR30977596.bw"
            if bigwig_file.exists():
                size_mb = bigwig_file.stat().st_size / (1024 * 1024)
                logger.info(f"  - BigWig: {bigwig_file} ({size_mb:.2f}MB)")
            
            coverage_summary = Config.RESULTS_DIR / "coverage_summary.json"
            if coverage_summary.exists():
                logger.info(f"  - Coverage Summary: {coverage_summary}")
            
            coverage_plots = Config.RESULTS_DIR / "coverage_analysis_plots.png"
            if coverage_plots.exists():
                logger.info(f"  - Coverage Plots: {coverage_plots}")
        else:
            logger.warning("커버리지 계산에 실패했습니다.")
        
    except Exception as e:
        logger.error(f"커버리지 계산 중 오류 발생: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark 세션 종료")

if __name__ == "__main__":
    main()
