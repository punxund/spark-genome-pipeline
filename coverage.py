from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType
import logging
from pathlib import Path
from typing import List, Tuple, Optional
import tempfile
import shutil
import os
import json
import pandas as pd

from config import Config
from utils import run_command, check_tool_availability, create_temp_file, cleanup_temp_files

logger = logging.getLogger(__name__)

def run_coverage_udf(sample_id: str, bam_file: str) -> dict:
    """
    커버리지 계산을 실행하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        bam_file: BAM 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    try:
        # 임시 파일 생성
        coverage_bed = create_temp_file(suffix=".bed", prefix=f"{sample_id}_coverage_")
        sorted_bed = create_temp_file(suffix=".bed", prefix=f"{sample_id}_sorted_")
        bigwig_file = create_temp_file(suffix=".bw", prefix=f"{sample_id}_")
        
        temp_files.extend([coverage_bed, sorted_bed, bigwig_file])
        
        # 1. bedtools genomecov로 커버리지 계산
        coverage_cmd = [
            Config.BEDTOOLS_PATH, "genomecov",
            "-ibam", bam_file,
            "-bg"
        ]
        
        coverage_result = run_command(coverage_cmd, check=False)
        if coverage_result.returncode != 0:
            logger.error(f"커버리지 계산 실패: {sample_id} - {coverage_result.stderr}")
            return {
                "sample_id": sample_id,
                "status": "failed",
                "coverage_bed": None,
                "bigwig_file": None,
                "coverage_stats": None,
                "error": coverage_result.stderr
            }
        
        # 커버리지 결과를 파일에 저장
        with open(coverage_bed, 'w') as f:
            f.write(coverage_result.stdout)
        
        # 2. BED 파일 정렬
        sort_cmd = [
            Config.BEDTOOLS_PATH, "sort",
            "-i", str(coverage_bed)
        ]
        
        sort_result = run_command(sort_cmd, check=False)
        if sort_result.returncode == 0:
            with open(sorted_bed, 'w') as f:
                f.write(sort_result.stdout)
        else:
            logger.warning(f"BED 정렬 실패: {sample_id} - {sort_result.stderr}")
            # 정렬 실패 시 원본 파일 사용
            shutil.copy2(coverage_bed, sorted_bed)
        
        # 3. BigWig 파일 생성 (subprocess로만 실행)
        bigwig_created = False
        bigwig_cmd = [
            Config.BIGWIG_PATH,
            str(sorted_bed),
            str(Config.REFERENCE_INDEX),
            str(bigwig_file)
        ]
        
        bigwig_result = run_command(bigwig_cmd, check=False)
        if bigwig_result.returncode == 0:
            bigwig_created = True
            logger.info(f"BigWig 파일 생성 완료: {sample_id}")
        else:
            logger.warning(f"BigWig 파일 생성 실패: {sample_id} - {bigwig_result.stderr}")
        
        # 4. 커버리지 통계 계산
        coverage_stats = calculate_coverage_stats(sorted_bed)
        
        # 5. 결과 파일들을 결과 디렉토리로 복사
        final_bed = Config.RESULTS_DIR / f"{sample_id}_coverage.bed"
        final_bw = Config.RESULTS_DIR / f"{sample_id}.bw" if bigwig_created else None
        
        shutil.copy2(sorted_bed, final_bed)
        if bigwig_created:
            shutil.copy2(bigwig_file, final_bw)
        
        logger.info(f"커버리지 계산 완료: {sample_id}")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "coverage_bed": str(final_bed),
            "bigwig_file": str(final_bw) if bigwig_created else None,
            "coverage_stats": coverage_stats,
            "error": None
        }
            
    except Exception as e:
        logger.error(f"커버리지 계산 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "coverage_bed": None,
            "bigwig_file": None,
            "coverage_stats": None,
            "error": str(e)
        }
    finally:
        # 임시 파일 정리
        cleanup_temp_files(temp_files)

def calculate_coverage_stats(bed_file: Path) -> dict:
    """
    BED 파일에서 커버리지 통계를 계산합니다.
    
    Args:
        bed_file: BED 파일 경로
    
    Returns:
        커버리지 통계 딕셔너리
    """
    try:
        # BED 파일 읽기
        df = pd.read_csv(bed_file, sep='\t', header=None, 
                       names=['chrom', 'start', 'end', 'coverage'])
        
        # 통계 계산
        stats = {
            'total_regions': str(len(df)),
            'mean_coverage': str(float(df['coverage'].mean())),
            'median_coverage': str(float(df['coverage'].median())),
            'max_coverage': str(float(df['coverage'].max())),
            'min_coverage': str(float(df['coverage'].min())),
            'std_coverage': str(float(df['coverage'].std())),
            'total_bases_covered': str(int(df['coverage'].sum())),
            'bases_with_coverage': str(int((df['coverage'] > 0).sum()))
        }
        
        # 커버리지 분포 계산
        coverage_ranges = [
            (0, 1, 'no_coverage'),
            (1, 10, 'low_coverage'),
            (10, 50, 'medium_coverage'),
            (50, 100, 'high_coverage'),
            (100, float('inf'), 'very_high_coverage')
        ]
        
        for min_cov, max_cov, label in coverage_ranges:
            if max_cov == float('inf'):
                count = int((df['coverage'] >= min_cov).sum())
            else:
                count = int(((df['coverage'] >= min_cov) & (df['coverage'] < max_cov)).sum())
            stats[f'{label}_regions'] = str(count)
        
        return stats
        
    except Exception as e:
        logger.warning(f"커버리지 통계 계산 실패: {str(e)}")
        return {}

class CoverageCalculator:
    """Spark를 사용한 커버리지 계산 클래스"""
    
    def __init__(self, spark: SparkSession, reference_index: Path = None):
        self.spark = spark
        self.temp_files = []
        self.reference_index = reference_index or Config.REFERENCE_INDEX
        
        # bedtools 도구 사용 가능 여부 확인
        if not check_tool_availability("bedtools", Config.BEDTOOLS_PATH):
            raise RuntimeError("bedtools 도구를 찾을 수 없습니다. 설치 후 다시 시도하세요.")
        
        # 참조 인덱스 파일 존재 확인
        if not self.reference_index.exists():
            raise FileNotFoundError(f"참조 인덱스 파일을 찾을 수 없습니다: {self.reference_index}")
    
    def process_coverage(self, sam_processed_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        """
        BAM 파일들의 커버리지를 계산합니다.
        
        Args:
            sam_processed_df: SAM 처리 결과 DataFrame
        
        Returns:
            커버리지 계산 결과 DataFrame
        """
        logger.info("커버리지 계산 시작")
        
        # 성공한 SAM 처리 결과만 필터링
        successful_df = sam_processed_df.filter(
            col("samtools_result.status") == "success"
        )
        
        if successful_df.count() == 0:
            logger.warning("커버리지를 계산할 BAM 파일이 없습니다.")
            return self.spark.createDataFrame([], StructType([
                StructField("sample_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("coverage_bed", StringType(), True),
                StructField("bigwig_file", StringType(), True),
                StructField("coverage_stats", MapType(StringType(), StringType()), True),
                StructField("error", StringType(), True)
            ]))
        
        # UDF 등록
        coverage_udf = udf(run_coverage_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("coverage_bed", StringType(), True),
            StructField("bigwig_file", StringType(), True),
            StructField("coverage_stats", MapType(StringType(), StringType()), True),
            StructField("error", StringType(), True)
        ]))
        
        # 커버리지 계산 실행
        result_df = successful_df.withColumn(
            "coverage_result",
            coverage_udf(
                col("samtools_result.sample_id"),
                col("samtools_result.bam_file")
            )
        )
        
        # 결과 확인
        success_count = result_df.filter(col("coverage_result.status") == "success").count()
        total_count = result_df.count()
        
        logger.info(f"커버리지 계산 완료: {success_count}/{total_count} 성공")
        
        return result_df
    
    def generate_coverage_report(self, coverage_df: "pyspark.sql.DataFrame") -> dict:
        """
        커버리지 계산 결과 요약 보고서를 생성합니다.
        
        Args:
            coverage_df: 커버리지 계산 결과 DataFrame
        
        Returns:
            요약 보고서 딕셔너리
        """
        logger.info("커버리지 요약 보고서 생성")
        
        summary = {
            "total_samples": coverage_df.count(),
            "successful_samples": coverage_df.filter(col("coverage_result.status") == "success").count(),
            "failed_samples": coverage_df.filter(col("coverage_result.status") == "failed").count(),
            "error_samples": coverage_df.filter(col("coverage_result.status") == "error").count(),
            "samples": []
        }
        
        # 성공한 샘플들의 통계 수집
        successful_samples = coverage_df.filter(col("coverage_result.status") == "success")
        
        for row in successful_samples.collect():
            sample_info = {
                "sample_id": row["coverage_result"]["sample_id"],
                "coverage_bed": row["coverage_result"]["coverage_bed"],
                "bigwig_file": row["coverage_result"]["bigwig_file"],
                "coverage_stats": row["coverage_result"]["coverage_stats"]
            }
            summary["samples"].append(sample_info)
        
        # 요약 보고서 저장
        report_file = Config.RESULTS_DIR / "coverage_summary.json"
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"커버리지 요약 보고서 저장: {report_file}")
        
        return summary
    

    
    def cleanup(self):
        """임시 파일들을 정리합니다."""
        cleanup_temp_files(self.temp_files)
        self.temp_files.clear()

def run_coverage_calculation(spark: SparkSession, sam_processed_df: "pyspark.sql.DataFrame", 
                           reference_index: Path = None) -> "pyspark.sql.DataFrame":
    """
    커버리지 계산 파이프라인을 실행합니다.
    
    Args:
        spark: SparkSession
        sam_processed_df: SAM 처리 결과 DataFrame
        reference_index: 참조 인덱스 파일 경로 (기본값: Config.REFERENCE_INDEX)
    
    Returns:
        커버리지 계산 결과 DataFrame
    """
    if reference_index is None:
        reference_index = Config.REFERENCE_INDEX
    
    # 커버리지 계산 실행
    calculator = CoverageCalculator(spark, reference_index)
    try:
        result_df = calculator.process_coverage(sam_processed_df)
        
        # 요약 보고서 생성
        if result_df.count() > 0:
            calculator.generate_coverage_report(result_df)
        
        return result_df
    finally:
        calculator.cleanup()
