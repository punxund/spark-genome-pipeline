from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
from pathlib import Path
from typing import List, Tuple, Optional
import tempfile
import shutil
import os
import json

from config import Config
from utils import run_command, check_tool_availability, create_temp_file, cleanup_temp_files

logger = logging.getLogger(__name__)

def run_samtools_udf(sample_id: str, sam_file: str) -> dict:
    """
    samtools를 실행하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        sam_file: SAM 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    try:
        # 임시 파일 생성
        sorted_bam = create_temp_file(suffix=".bam", prefix=f"{sample_id}_sorted_")
        stats_file = create_temp_file(suffix=".txt", prefix=f"{sample_id}_stats_")
        
        temp_files.extend([sorted_bam, stats_file])
        
        # 1. SAM을 BAM으로 변환하고 정렬
        sort_cmd = [
            Config.SAMTOOLS_PATH, "sort",
            "-o", str(sorted_bam),
            sam_file
        ]
        
        sort_result = run_command(sort_cmd, check=False)
        if sort_result.returncode != 0:
            logger.error(f"BAM 정렬 실패: {sample_id} - {sort_result.stderr}")
            return {
                "sample_id": sample_id,
                "status": "failed",
                "bam_file": None,
                "stats_file": None,
                "mapping_stats": None,
                "error": sort_result.stderr
            }
        
        # 2. 매핑 통계 생성
        stats_cmd = [
            Config.SAMTOOLS_PATH, "flagstat",
            str(sorted_bam)
        ]
        
        stats_result = run_command(stats_cmd, check=False)
        if stats_result.returncode == 0:
            # 통계를 파일에 저장
            with open(stats_file, 'w') as f:
                f.write(stats_result.stdout)
            
            # 통계 파싱
            mapping_stats = parse_flagstat(stats_result.stdout)
        else:
            logger.warning(f"통계 생성 실패: {sample_id} - {stats_result.stderr}")
            mapping_stats = None
        
        # 3. 결과 파일들을 결과 디렉토리로 복사
        final_bam = Config.RESULTS_DIR / f"{sample_id}_filtered.bam"
        final_stats = Config.RESULTS_DIR / f"{sample_id}_mapping_stats.txt"
        
        shutil.copy2(sorted_bam, final_bam)
        if mapping_stats:
            shutil.copy2(stats_file, final_stats)
        
        logger.info(f"samtools 처리 완료: {sample_id}")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "bam_file": str(final_bam),
            "stats_file": str(final_stats) if mapping_stats else None,
            "mapping_stats": mapping_stats,
            "error": None
        }
            
    except Exception as e:
        logger.error(f"samtools 처리 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "bam_file": None,
            "stats_file": None,
            "mapping_stats": None,
            "error": str(e)
        }
    finally:
        # 임시 파일 정리
        cleanup_temp_files(temp_files)

def parse_flagstat(flagstat_output: str) -> dict:
    """
    samtools flagstat 출력을 파싱합니다.
    
    Args:
        flagstat_output: flagstat 명령어 출력
    
    Returns:
        파싱된 통계 딕셔너리
    """
    stats = {}
    lines = flagstat_output.strip().split('\n')
    
    for line in lines:
        if 'total' in line:
            stats['total_reads'] = int(line.split()[0])
        elif 'mapped' in line and 'mapped (' in line:
            stats['mapped_reads'] = int(line.split()[0])
        elif 'paired in sequencing' in line:
            stats['paired_reads'] = int(line.split()[0])
        elif 'properly paired' in line:
            stats['properly_paired'] = int(line.split()[0])
        elif 'singletons' in line:
            stats['singletons'] = int(line.split()[0])
        elif 'with itself and mate mapped' in line:
            stats['both_mapped'] = int(line.split()[0])
        elif 'with mate mapped to a different chr' in line:
            stats['different_chr'] = int(line.split()[0])
        elif 'with mate mapped to a different chr (mapQ>=5)' in line:
            stats['different_chr_mapq5'] = int(line.split()[0])
    
    # 매핑률 계산
    if 'total_reads' in stats and 'mapped_reads' in stats:
        stats['mapping_rate'] = (stats['mapped_reads'] / stats['total_reads']) * 100
    
    return stats

class SAMProcessor:
    """Spark를 사용한 SAM 파일 처리 클래스"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.temp_files = []
        
        # samtools 도구 사용 가능 여부 확인
        if not check_tool_availability("samtools", Config.SAMTOOLS_PATH):
            raise RuntimeError("samtools 도구를 찾을 수 없습니다. 설치 후 다시 시도하세요.")
    
    def process_sam_files(self, alignment_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        """
        SAM 파일들을 처리합니다.
        
        Args:
            alignment_df: BWA 매핑 결과 DataFrame
        
        Returns:
            처리 결과 DataFrame
        """
        logger.info("SAM 파일 처리 시작")
        
        # 성공한 매핑 결과만 필터링
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
                StructField("mapping_stats", StringType(), True),
                StructField("error", StringType(), True)
            ]))
        
        # UDF 등록
        samtools_udf = udf(run_samtools_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("bam_file", StringType(), True),
            StructField("stats_file", StringType(), True),
            StructField("mapping_stats", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # samtools 실행
        result_df = successful_df.withColumn(
            "samtools_result",
            samtools_udf(
                col("bwa_result.sample_id"),
                col("bwa_result.sam_file")
            )
        )
        
        # 결과 확인
        success_count = result_df.filter(col("samtools_result.status") == "success").count()
        total_count = result_df.count()
        
        logger.info(f"SAM 처리 완료: {success_count}/{total_count} 성공")
        
        return result_df
    
    def generate_summary_report(self, processed_df: "pyspark.sql.DataFrame") -> dict:
        """
        처리 결과 요약 보고서를 생성합니다.
        
        Args:
            processed_df: SAM 처리 결과 DataFrame
        
        Returns:
            요약 보고서 딕셔너리
        """
        logger.info("요약 보고서 생성")
        
        summary = {
            "total_samples": processed_df.count(),
            "successful_samples": processed_df.filter(col("samtools_result.status") == "success").count(),
            "failed_samples": processed_df.filter(col("samtools_result.status") == "failed").count(),
            "error_samples": processed_df.filter(col("samtools_result.status") == "error").count(),
            "samples": []
        }
        
        # 성공한 샘플들의 통계 수집
        successful_samples = processed_df.filter(col("samtools_result.status") == "success")
        
        for row in successful_samples.collect():
            sample_info = {
                "sample_id": row["samtools_result"]["sample_id"],
                "bam_file": row["samtools_result"]["bam_file"],
                "stats_file": row["samtools_result"]["stats_file"],
                "mapping_stats": row["samtools_result"]["mapping_stats"]
            }
            summary["samples"].append(sample_info)
        
        # 요약 보고서 저장
        report_file = Config.RESULTS_DIR / "sam_processing_summary.json"
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"요약 보고서 저장: {report_file}")
        
        return summary
    
    def cleanup(self):
        """임시 파일들을 정리합니다."""
        cleanup_temp_files(self.temp_files)
        self.temp_files.clear()

def run_sam_processing(spark: SparkSession, alignment_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    SAM 처리 파이프라인을 실행합니다.
    
    Args:
        spark: SparkSession
        alignment_df: BWA 매핑 결과 DataFrame
    
    Returns:
        SAM 처리 결과 DataFrame
    """
    # SAM 처리 실행
    processor = SAMProcessor(spark)
    try:
        result_df = processor.process_sam_files(alignment_df)
        
        # 요약 보고서 생성
        if result_df.count() > 0:
            processor.generate_summary_report(result_df)
        
        return result_df
    finally:
        processor.cleanup()
