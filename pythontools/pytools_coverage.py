#!/usr/bin/env python3
"""
Python 네이티브 도구들을 사용한 커버리지 계산 모듈
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
# defaultdict 대신 일반 dict 사용

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config_pytools import PyToolsConfig

logger = logging.getLogger(__name__)

def calculate_coverage_with_python(bam_file: str, reference_index: str, window_size: int = 1000) -> Tuple[List[str], Dict[str, Any]]:
    """
    Python 네이티브로 커버리지 계산
    
    Args:
        bam_file: BAM 파일 경로
        reference_index: 참조 인덱스 파일 경로
        window_size: 윈도우 크기
    
    Returns:
        커버리지 데이터와 통계
    """
    # 참조 게놈 크기 읽기
    chrom_sizes = {}
    with open(reference_index, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                chrom_sizes[parts[0]] = int(parts[1])
    
    # BAM 파일 읽기 (실제로는 SAM 형식)
    coverage_data = {}  # defaultdict 대신 일반 dict 사용
    total_reads = 0
    
    with open(bam_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line.startswith('@'):
                parts = line.split('\t')
                if len(parts) >= 4:
                    chrom = parts[2]
                    if chrom in chrom_sizes:
                        pos = int(parts[3])
                        # 윈도우 기반 커버리지 계산
                        window_start = (pos // window_size) * window_size
                        key = f"{chrom}:{window_start}"
                        coverage_data[key] = coverage_data.get(key, 0) + 1
                        total_reads += 1
    
    # BED 형식으로 변환
    bed_records = []
    for key, count in coverage_data.items():
        chrom, start = key.split(':')
        start = int(start)
        end = start + window_size
        bed_records.append(f"{chrom}\t{start}\t{end}\t{count}")
    
    # 통계 계산
    stats = {
        "total_reads": total_reads,
        "covered_windows": len(coverage_data),
        "total_windows": sum(chrom_sizes.values()) // window_size,
        "coverage_rate": (len(coverage_data) / (sum(chrom_sizes.values()) // window_size)) * 100 if chrom_sizes else 0,
        "average_coverage": sum(coverage_data.values()) / len(coverage_data) if coverage_data else 0
    }
    
    return bed_records, stats

def create_bigwig_from_bed(bed_records: List[str], chrom_sizes: Dict[str, int], bigwig_file: str) -> bool:
    """
    BED 데이터로부터 BigWig 파일 생성
    
    Args:
        bed_records: BED 형식 레코드들
        chrom_sizes: 염색체 크기 정보
        bigwig_file: 출력 BigWig 파일 경로
    
    Returns:
        성공 여부
    """
    try:
        # 간단한 BigWig 형식으로 저장 (실제로는 텍스트 파일)
        with open(bigwig_file, 'w') as f:
            f.write("# BigWig-like format (simplified)\n")
            f.write(f"# Total records: {len(bed_records)}\n")
            for record in bed_records:
                f.write(record + '\n')
        
        return True
    except Exception as e:
        logger.error(f"BigWig 파일 생성 실패: {e}")
        return False

def run_python_coverage_udf(sample_id: str, bam_file: str, reference_index: str) -> dict:
    """
    Python 네이티브 커버리지 계산을 실행하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        bam_file: BAM 파일 경로
        reference_index: 참조 인덱스 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    try:
        config = PyToolsConfig.COVERAGE_CONFIG
        
        # 커버리지 계산
        bed_records, stats = calculate_coverage_with_python(
            bam_file, 
            reference_index, 
            window_size=config["window_size"]
        )
        
        # BED 파일 저장
        bed_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_coverage.bed"
        with open(bed_file, 'w') as f:
            for record in bed_records:
                f.write(record + '\n')
        
        # BigWig 파일 생성
        bigwig_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}.bw"
        
        # 참조 게놈 크기 읽기
        chrom_sizes = {}
        with open(reference_index, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    chrom_sizes[parts[0]] = int(parts[1])
        
        bigwig_success = create_bigwig_from_bed(bed_records, chrom_sizes, str(bigwig_file))
        
        # 통계 파일 저장
        stats_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_python_coverage_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Python 네이티브 커버리지 계산 완료: {sample_id}")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "bed_file": str(bed_file),
            "bigwig_file": str(bigwig_file) if bigwig_success else None,
            "stats_file": str(stats_file),
            "stats": stats,
            "error": None
        }
            
    except Exception as e:
        logger.error(f"Python 네이티브 커버리지 계산 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "bed_file": None,
            "bigwig_file": None,
            "stats_file": None,
            "stats": None,
            "error": str(e)
        }
    finally:
        # 임시 파일 정리
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

class PyToolsCoverageCalculator:
    """Python 네이티브 도구들을 사용한 커버리지 계산 클래스"""
    
    def __init__(self, spark: SparkSession, reference_index: Path = None):
        self.spark = spark
        self.reference_index = reference_index or PyToolsConfig.REFERENCE_INDEX
        self.temp_files = []
        
        # 참조 인덱스 확인
        if not self.reference_index.exists():
            raise FileNotFoundError(f"참조 인덱스 파일을 찾을 수 없습니다: {self.reference_index}")
    
    def calculate_coverage(self, sam_processed_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        """
        SAM 처리 결과로부터 커버리지를 계산합니다.
        
        Args:
            sam_processed_df: SAM 처리 결과 DataFrame
        
        Returns:
            커버리지 계산 결과 DataFrame
        """
        logger.info("Python 네이티브 커버리지 계산 시작")
        
        # 성공한 SAM 처리 결과만 필터링
        successful_df = sam_processed_df.filter(
            col("samtools_result.status") == "success"
        )
        
        if successful_df.count() == 0:
            logger.warning("커버리지 계산할 BAM 파일이 없습니다.")
            return self.spark.createDataFrame([], StructType([
                StructField("sample_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("bed_file", StringType(), True),
                StructField("bigwig_file", StringType(), True),
                StructField("stats_file", StringType(), True),
                StructField("stats", StringType(), True),
                StructField("error", StringType(), True)
            ]))
        
        # UDF 등록
        python_coverage_udf = udf(run_python_coverage_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("bed_file", StringType(), True),
            StructField("bigwig_file", StringType(), True),
            StructField("stats_file", StringType(), True),
            StructField("stats", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # Python 네이티브 커버리지 계산 실행
        result_df = successful_df.withColumn(
            "coverage_result",
            python_coverage_udf(
                col("samtools_result.sample_id"),
                col("samtools_result.bam_file"),
                lit(str(self.reference_index))
            )
        )
        
        logger.info("Python 네이티브 커버리지 계산 완료")
        return result_df

def run_coverage_calculation(spark: SparkSession, sam_processed_df: "pyspark.sql.DataFrame",
                           reference_index: Path = None) -> "pyspark.sql.DataFrame":
    """
    Python 네이티브 커버리지 계산을 실행합니다.
    
    Args:
        spark: SparkSession
        sam_processed_df: SAM 처리 결과 DataFrame
        reference_index: 참조 인덱스 파일
    
    Returns:
        커버리지 계산 결과 DataFrame
    """
    calculator = PyToolsCoverageCalculator(spark, reference_index)
    return calculator.calculate_coverage(sam_processed_df)
