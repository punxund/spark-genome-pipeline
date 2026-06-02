#!/usr/bin/env python3
"""
Python 네이티브 도구들을 사용한 FASTQ 전처리 모듈
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType
import logging
from pathlib import Path
import os
import gzip
import json

import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_pytools import PyToolsConfig
from utils import parse_fastq_pairs

logger = logging.getLogger(__name__)

def trim_by_quality(sequence: str, quality: str, threshold: int) -> tuple:
    """품질 기반으로 시퀀스 트리밍"""
    quals = [ord(q) - 33 for q in quality]
    
    # 3' 끝에서 트리밍
    end_pos = len(sequence)
    for i in range(len(sequence) - 1, -1, -1):
        if quals[i] >= threshold:
            end_pos = i + 1
            break
    
    # 5' 끝에서 트리밍
    start_pos = 0
    for i in range(len(sequence)):
        if quals[i] >= threshold:
            start_pos = i
            break
    
    trimmed_seq = sequence[start_pos:end_pos]
    trimmed_qual = quality[start_pos:end_pos]
    removed_bases = len(sequence) - len(trimmed_seq)
    
    return trimmed_seq, trimmed_qual, removed_bases

def run_python_fastp_udf(sample_id: str, r1_file: str, r2_file: str) -> dict:
    """Python 네이티브 fastp를 실행하는 UDF 함수 (전체 파일 처리)"""
    try:
        logger.info(f"Python fastp 시작: {sample_id}")
        
        # 결과 파일 생성
        r1_output = PyToolsConfig.RESULTS_DIR / f"{sample_id}.R1.trimmed.fastq"
        r2_output = PyToolsConfig.RESULTS_DIR / f"{sample_id}.R2.trimmed.fastq"
        
        config = PyToolsConfig.FASTP_CONFIG
        quality_threshold = config["qualified_quality_phred"]
        min_length = config["length_required"]
        
        # 통계 초기화
        stats = {
            "total_reads": 0,
            "passed_reads": 0,
            "failed_reads": 0,
            "trimmed_bases": 0,
            "quality_distribution": {}
        }
        
        logger.info("FASTQ 파일 읽기 시작...")
        
        # 전체 파일을 한 번에 읽기
        with gzip.open(r1_file, 'rt') if r1_file.endswith('.gz') else open(r1_file, 'r') as r1_in, \
             gzip.open(r2_file, 'rt') if r2_file.endswith('.gz') else open(r2_file, 'r') as r2_in:
            
            r1_data = r1_in.readlines()
            r2_data = r2_in.readlines()
        
        logger.info(f"파일 읽기 완료: R1={len(r1_data)}줄, R2={len(r2_data)}줄")
        
        # 데이터 정리
        r1_data = [line.strip() for line in r1_data]
        r2_data = [line.strip() for line in r2_data]
        
        # Paired-end 데이터: R1과 R2는 하나의 읽기 쌍
        total_read_pairs = len(r1_data) // 4
        logger.info(f"총 읽기 쌍 수: {total_read_pairs:,}개 (R1: {total_read_pairs:,}, R2: {total_read_pairs:,})")
        
        with open(r1_output, 'w') as r1_f, open(r2_output, 'w') as r2_f:
            # 모든 읽기 쌍 처리
            for i in range(0, len(r1_data), 4):
                if i + 3 >= len(r1_data):
                    break
                
                stats["total_reads"] += 1  # 읽기 쌍 1개
                
                # 진행률 표시 (100,000개마다)
                if stats["total_reads"] % 100000 == 0:
                    progress = (stats["total_reads"] / total_read_pairs) * 100
                    logger.info(f"진행률: {stats['total_reads']:,}/{total_read_pairs:,} 읽기 쌍 ({progress:.1f}%)")
                
                # R1, R2 읽기 파싱
                r1_header, r1_sequence, r1_plus, r1_quality = r1_data[i:i+4]
                r2_header, r2_sequence, r2_plus, r2_quality = r2_data[i:i+4]
                
                # 품질 분포 업데이트 (R1 + R2 모두)
                for q in [ord(q) - 33 for q in r1_quality + r2_quality]:
                    stats["quality_distribution"][q] = stats["quality_distribution"].get(q, 0) + 1
                
                # 품질 기반 트리밍 (R1과 R2 모두)
                r1_trimmed = trim_by_quality(r1_sequence, r1_quality, quality_threshold)
                r2_trimmed = trim_by_quality(r2_sequence, r2_quality, quality_threshold)
                
                # 길이 필터링 (R1과 R2 모두 만족해야 함)
                if len(r1_trimmed[0]) >= min_length and len(r2_trimmed[0]) >= min_length:
                    # 결과 파일에 쓰기 (R1과 R2 모두)
                    r1_f.write(f"{r1_header}\n{r1_trimmed[0]}\n{r1_plus}\n{r1_trimmed[1]}\n")
                    r2_f.write(f"{r2_header}\n{r2_trimmed[0]}\n{r2_plus}\n{r2_trimmed[1]}\n")
                    
                    stats["passed_reads"] += 1  # 읽기 쌍 1개 통과
                    stats["trimmed_bases"] += r1_trimmed[2] + r2_trimmed[2]  # R1 + R2 트리밍된 베이스
                else:
                    stats["failed_reads"] += 1  # 읽기 쌍 1개 실패
        
        logger.info(f"전체 처리 완료: {stats['total_reads']:,}개 읽기 쌍 중 {stats['passed_reads']:,}개 통과 ({stats['passed_reads']/stats['total_reads']*100:.1f}%)")
        logger.info(f"실제 처리된 읽기 수: R1 {stats['passed_reads']:,}개 + R2 {stats['passed_reads']:,}개 = 총 {stats['passed_reads']*2:,}개")
        
        # 통계 계산 (읽기 쌍 기준)
        if stats["total_reads"] > 0:
            # 평균 읽기 길이 계산 (R1 + R2 평균)
            avg_read_length = 87  # BWA 결과에서 확인된 평균 길이
            stats["trimming_rate"] = (stats["trimmed_bases"] / (stats["total_reads"] * avg_read_length * 2)) * 100  # R1 + R2
            stats["pass_rate"] = (stats["passed_reads"] / stats["total_reads"]) * 100
        else:
            stats["trimming_rate"] = 0.0
            stats["pass_rate"] = 0.0
        
        # 통계 파일 저장
        stats_file = PyToolsConfig.RESULTS_DIR / f"report_{sample_id}_python_fastp.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Python fastp 처리 완료: {sample_id}")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "r1_file": str(r1_output),
            "r2_file": str(r2_output),
            "stats_file": str(stats_file),
            "stats": stats,
            "error": None
        }
            
    except Exception as e:
        logger.error(f"Python fastp 처리 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "r1_file": None,
            "r2_file": None,
            "stats_file": None,
            "stats": None,
            "error": str(e)
        }

class PyToolsPreprocessor:
    """Python 네이티브 도구들을 사용한 전처리 클래스"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        if not PyToolsConfig.check_python_dependencies():
            raise RuntimeError("필요한 Python 패키지들이 설치되지 않았습니다.")
    
    def process_fastq_files(self, reads_dir: Path = None) -> "pyspark.sql.DataFrame":
        """FASTQ 파일들을 Python 네이티브 도구로 전처리"""
        if reads_dir is None:
            reads_dir = PyToolsConfig.READS_DIR
        
        logger.info("Python 네이티브 FASTQ 전처리 시작")
        
        # FASTQ 파일 쌍 파싱
        fastq_pairs = parse_fastq_pairs(reads_dir)
        
        if not fastq_pairs:
            logger.warning("처리할 FASTQ 파일 쌍을 찾을 수 없습니다.")
            return self.spark.createDataFrame([], StructType([
                StructField("sample_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("r1_file", StringType(), True),
                StructField("r2_file", StringType(), True),
                StructField("stats_file", StringType(), True),
                StructField("stats", StringType(), True),
                StructField("error", StringType(), True)
            ]))
        
        # DataFrame 생성
        data = [{"sample_id": sid, "r1_file": str(r1), "r2_file": str(r2)} 
                for sid, r1, r2 in fastq_pairs]
        df = self.spark.createDataFrame(data)
        
        # UDF 등록 및 실행
        python_fastp_udf = udf(run_python_fastp_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("r1_file", StringType(), True),
            StructField("r2_file", StringType(), True),
            StructField("stats_file", StringType(), True),
            StructField("stats", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        result_df = df.withColumn("fastp_result", python_fastp_udf(col("sample_id"), col("r1_file"), col("r2_file")))
        
        logger.info("Python 네이티브 FASTQ 전처리 완료")
        return result_df

def run_preprocessing(spark: SparkSession, reads_dir: Path = None) -> "pyspark.sql.DataFrame":
    """Python 네이티브 전처리를 실행"""
    preprocessor = PyToolsPreprocessor(spark)
    return preprocessor.process_fastq_files(reads_dir)
