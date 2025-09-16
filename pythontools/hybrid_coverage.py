#!/usr/bin/env python3
"""
Hybrid 커버리지 계산 모듈 (pybedtools + pybigwig 활용)
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

def calculate_coverage_with_pybedtools(bam_file: str, reference_index: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    pybedtools를 사용하여 커버리지 계산 (Original과 동일한 방식)
    
    Args:
        bam_file: BAM 파일 경로
        reference_index: 참조 인덱스 파일 경로
    
    Returns:
        커버리지 데이터와 통계
    """
    try:
        import pybedtools
        
        # BAM 파일을 pybedtools로 읽기
        bam = pybedtools.BedTool(bam_file)
        
        # 게놈 커버리지 계산 (bedGraph 형식) - Original과 동일
        coverage = bam.genome_coverage(bg=True, g=reference_index)
        
        # 베이스별 커버리지를 BED 형식으로 변환 (윈도우 없이)
        bed_records = []
        total_regions = 0
        
        for interval in coverage:
            chrom = interval.chrom
            start = interval.start
            end = interval.end
            coverage_val = float(interval.name) if interval.name else 0
            
            # Original과 동일한 BED 형식
            bed_records.append(f"{chrom}\t{start}\t{end}\t{coverage_val}")
            total_regions += 1
        
        # 통계 계산 (Original과 동일한 방식)
        coverage_values = [float(record.split('\t')[3]) for record in bed_records] if bed_records else []
        
        stats = {
            "total_regions": total_regions,
            "total_bases_covered": int(sum(coverage_values)),  # Original과 동일: coverage 값의 합
            "mean_coverage": sum(coverage_values) / len(coverage_values) if coverage_values else 0,  # Original과 동일: 평균
            "median_coverage": "N/A",  # 계산 복잡성으로 인해 생략
            "max_coverage": max(coverage_values) if coverage_values else 0,
            "min_coverage": min(coverage_values) if coverage_values else 0
        }
        
        return bed_records, stats
        
    except ImportError:
        logger.error("pybedtools 라이브러리가 설치되지 않았습니다.")
        return [], {}
    except Exception as e:
        logger.error(f"pybedtools 커버리지 계산 중 오류: {str(e)}")
        return [], {}

def create_bigwig_with_pybigwig(bed_records: List[str], chrom_sizes: Dict[str, int], bigwig_file: str) -> bool:
    """
    pybigwig를 사용하여 BigWig 파일 생성
    
    Args:
        bed_records: BED 형식 레코드들
        chrom_sizes: 염색체 크기 정보
        bigwig_file: 출력 BigWig 파일 경로
    
    Returns:
        성공 여부
    """
    try:
        import pyBigWig
        
        # BigWig 파일 생성
        bw = pyBigWig.open(bigwig_file, 'w')
        
        # 헤더 작성
        bw.addHeader(list(chrom_sizes.items()))
        
        # 커버리지 데이터 추가
        for record in bed_records:
            parts = record.strip().split('\t')
            if len(parts) >= 4:
                chrom = parts[0]
                start = int(parts[1])
                end = int(parts[2])
                coverage_val = float(parts[3])
                
                bw.addEntries([chrom], [start], [end], [coverage_val])
        
        bw.close()
        logger.info(f"BigWig 파일 생성 완료: {bigwig_file}")
        return True
        
    except ImportError:
        logger.error("pybigwig 라이브러리가 설치되지 않았습니다.")
        return False
    except Exception as e:
        logger.error(f"BigWig 파일 생성 실패: {e}")
        return False

def run_pybedtools_coverage_udf(sample_id: str, bam_file: str, reference_index: str) -> dict:
    """
    pybedtools + pybigwig를 사용한 커버리지 계산을 실행하는 UDF 함수
    
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
        
        # pybedtools로 커버리지 계산 (Original과 동일한 방식)
        bed_records, stats = calculate_coverage_with_pybedtools(
            bam_file, 
            reference_index
        )
        
        if not bed_records:
            return {
                "sample_id": sample_id,
                "status": "error",
                "bed_file": None,
                "bigwig_file": None,
                "stats_file": None,
                "stats": None,
                "error": "커버리지 계산에 실패했습니다."
            }
        
        # BED 파일 저장
        bed_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_coverage.bed"
        with open(bed_file, 'w') as f:
            for record in bed_records:
                f.write(record + '\n')
        
        # 참조 게놈 크기 읽기
        chrom_sizes = {}
        with open(reference_index, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    chrom_sizes[parts[0]] = int(parts[1])
        
        # pybigwig로 BigWig 파일 생성
        bigwig_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}.bw"
        bigwig_success = create_bigwig_with_pybigwig(bed_records, chrom_sizes, str(bigwig_file))
        
        # 통계 파일 저장
        stats_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_pybedtools_coverage_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"pybedtools + pybigwig 커버리지 계산 완료: {sample_id}")
        logger.info(f"  - 총 영역: {stats.get('total_regions', 0)}")
        logger.info(f"  - 평균 커버리지: {stats.get('mean_coverage', 0):.2f}")
        
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
        logger.error(f"pybedtools + pybigwig 커버리지 계산 중 예외 발생: {sample_id} - {str(e)}")
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

class HybridCoverageCalculator:
    """pybedtools + pybigwig를 사용한 커버리지 계산 클래스"""
    
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
        logger.info("pybedtools + pybigwig 커버리지 계산 시작")
        
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
        pybedtools_coverage_udf = udf(run_pybedtools_coverage_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("bed_file", StringType(), True),
            StructField("bigwig_file", StringType(), True),
            StructField("stats_file", StringType(), True),
            StructField("stats", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # pybedtools + pybigwig 커버리지 계산 실행
        result_df = successful_df.withColumn(
            "coverage_result",
            pybedtools_coverage_udf(
                col("samtools_result.sample_id"),
                col("samtools_result.bam_file"),
                lit(str(self.reference_index))
            )
        )
        
        logger.info("pybedtools + pybigwig 커버리지 계산 완료")
        return result_df

def run_coverage_calculation(spark: SparkSession, sam_processed_df: "pyspark.sql.DataFrame",
                           reference_index: Path = None) -> "pyspark.sql.DataFrame":
    """
    pybedtools + pybigwig를 사용한 커버리지 계산을 실행합니다.
    
    Args:
        spark: SparkSession
        sam_processed_df: SAM 처리 결과 DataFrame
        reference_index: 참조 인덱스 파일 경로
    
    Returns:
        커버리지 계산 결과 DataFrame
    """
    calculator = HybridCoverageCalculator(spark, reference_index)
    return calculator.calculate_coverage(sam_processed_df)
