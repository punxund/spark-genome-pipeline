#!/usr/bin/env python3
"""
Minimap2를 사용한 읽기 매핑 모듈
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
import gzip
import subprocess
import threading
import time

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config_pytools import PyToolsConfig

logger = logging.getLogger(__name__)

def run_minimap2_alignment_udf(sample_id: str, r1_file: str, r2_file: str, reference_genome: str) -> dict:
    """
    전처리된 파일을 사용하여 Minimap2 정렬을 실행하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        r1_file: 전처리된 R1 파일 경로
        r2_file: 전처리된 R2 파일 경로
        reference_genome: 참조 게놈 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    try:
        logger.info(f"Minimap2 매핑 시작: {sample_id}")
        logger.info(f"R1 파일: {r1_file}")
        logger.info(f"R2 파일: {r2_file}")
        
        # 입력 파일 크기 확인
        r1_size = os.path.getsize(r1_file) if os.path.exists(r1_file) else 0
        r2_size = os.path.getsize(r2_file) if os.path.exists(r2_file) else 0
        logger.info(f"입력 파일 크기 - R1: {r1_size:,} bytes, R2: {r2_size:,} bytes")
        
        # 참조 게놈 크기 확인
        ref_size = os.path.getsize(reference_genome) if os.path.exists(reference_genome) else 0
        logger.info(f"참조 게놈 크기: {ref_size:,} bytes")
        
        # 전처리된 파일 존재 확인
        if not os.path.exists(r1_file) or not os.path.exists(r2_file):
            logger.error(f"전처리된 파일이 존재하지 않습니다: {r1_file}, {r2_file}")
            return {
                "sample_id": sample_id,
                "status": "error",
                "sam_file": None,
                "bam_file": None,
                "sorted_bam_file": None,
                "stats_file": None,
                "stats": None,
                "error": "전처리된 파일이 존재하지 않습니다"
            }
        
        # 출력 파일 경로 설정
        sam_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}.sam"
        bam_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}.bam"
        sorted_bam_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}.sorted.bam"
        
        # 기존 파일이 있으면 삭제
        for file_path in [sam_file, bam_file, sorted_bam_file]:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"기존 파일 삭제: {file_path}")
        
        # Minimap2 명령어 구성 (매핑률 향상을 위한 최적화된 설정)
        # 먼저 더 관대한 설정으로 시도
        minimap2_cmd = [
            "/vol/fob-vol7/mi21/kimhongs/anaconda3/bin/minimap2",
            "-ax", "sr",  # paired-end short reads
            "-t", "8",    # 스레드 수
            "-K", "4G",   # 메모리 제한 증가
            "-I", "32G",  # 인덱스 메모리 증가
            "-r", "0.00001", # 매핑률 임계값 극도로 완화
            "-N", "500",  # 최대 secondary hits 대폭 증가
            "-p", "0.0001", # 정확도 임계값 극도로 완화
            "-A", "1",    # 매치 점수
            "-B", "0",    # 불일치 페널티 최소화
            "-O", "4,16",  # 갭 페널티 (O1 < O2)
            "-E", "2,1",  # 갭 확장 페널티 (E1 > E2)
            "-z", "2000", # Z-drop 대폭 증가
            "-s", "15",   # 최소 시드 길이 더 감소
            "-f", "0.00001", # 최소 fragment 길이 비율
            "-F", "0.00001", # 최대 fragment 길이 비율
            "-G", "5000", # 최대 gap 길이 대폭 증가
            "-X",         # 더 나은 paired-end 매핑
            "--secondary=yes",  # secondary alignments 허용
            "--max-chain-skip", "50",  # 체인 스킵 대폭 증가
            "--max-chain-iter", "10000",  # 체인 반복 대폭 증가
            "--no-long-join",  # 긴 조인 비활성화
            "--lj-min-ratio", "0.1",  # 조인 비율 임계값 완화
            "-o", str(sam_file),
            str(reference_genome),
            str(r1_file),
            str(r2_file)
        ]
        
        logger.info(f"Minimap2 명령어 실행: {' '.join(minimap2_cmd)}")
        
        # Minimap2 실행
        start_time = time.time()
        result = subprocess.run(
            minimap2_cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1시간 타임아웃
        )
        minimap2_time = time.time() - start_time
        
        if result.returncode != 0:
            logger.error(f"Minimap2 실행 실패: {result.stderr}")
            return {
                "sample_id": sample_id,
                "status": "error",
                "sam_file": None,
                "bam_file": None,
                "sorted_bam_file": None,
                "stats_file": None,
                "stats": None,
                "error": f"Minimap2 실행 실패: {result.stderr}"
            }
        
        # SAM 파일이 생성되었는지 확인
        if not sam_file.exists():
            logger.error(f"SAM 파일이 생성되지 않았습니다: {sam_file}")
            return {
                "sample_id": sample_id,
                "status": "error",
                "sam_file": None,
                "bam_file": None,
                "sorted_bam_file": None,
                "stats_file": None,
                "stats": None,
                "error": "SAM 파일이 생성되지 않았습니다"
            }
        
        # SAM 파일 크기 확인
        sam_size = sam_file.stat().st_size
        logger.info(f"SAM 파일 크기: {sam_size:,} bytes")
        
        # SAM 파일이 너무 작으면 매핑이 제대로 되지 않았을 가능성
        if sam_size < 1000:  # 1KB 미만
            logger.warning(f"SAM 파일이 너무 작습니다 ({sam_size:,} bytes). 매핑이 제대로 되지 않았을 수 있습니다.")
        
        logger.info(f"Minimap2 매핑 완료: {minimap2_time:.2f}초")
        
        # SAM을 BAM으로 변환
        samtools_view_cmd = [
            "samtools", "view",
            "-b",  # BAM 형식
            "-o", str(bam_file),
            str(sam_file)
        ]
        
        logger.info("SAM을 BAM으로 변환 중...")
        result = subprocess.run(samtools_view_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"SAM to BAM 변환 실패: {result.stderr}")
            return {
                "sample_id": sample_id,
                "status": "error",
                "sam_file": str(sam_file),
                "bam_file": None,
                "sorted_bam_file": None,
                "stats_file": None,
                "stats": None,
                "error": f"SAM to BAM 변환 실패: {result.stderr}"
            }
        
        # BAM 정렬
        samtools_sort_cmd = [
            "samtools", "sort",
            "-o", str(sorted_bam_file),
            str(bam_file)
        ]
        
        logger.info("BAM 정렬 중...")
        result = subprocess.run(samtools_sort_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"BAM 정렬 실패: {result.stderr}")
            return {
                "sample_id": sample_id,
                "status": "error",
                "sam_file": str(sam_file),
                "bam_file": str(bam_file),
                "sorted_bam_file": None,
                "stats_file": None,
                "stats": None,
                "error": f"BAM 정렬 실패: {result.stderr}"
            }
        
        # BAM 인덱스 생성
        samtools_index_cmd = [
            "samtools", "index",
            str(sorted_bam_file)
        ]
        
        logger.info("BAM 인덱스 생성 중...")
        result = subprocess.run(samtools_index_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.warning(f"BAM 인덱스 생성 실패: {result.stderr}")
        
        # 매핑 통계 계산
        samtools_flagstat_cmd = [
            "samtools", "flagstat",
            str(sorted_bam_file)
        ]
        
        logger.info("매핑 통계 계산 중...")
        result = subprocess.run(samtools_flagstat_cmd, capture_output=True, text=True)
        
        stats = {
            "total_reads": 0,
            "mapped_reads": 0,
            "properly_paired": 0,
            "mapping_rate": 0.0,
            "proper_pair_rate": 0.0,
            "minimap2_time": minimap2_time,
            "flagstat_output": result.stdout if result.returncode == 0 else ""
        }
        
        # flagstat 결과 파싱
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if 'total' in line and 'QC-passed' in line:
                    stats["total_reads"] = int(line.split()[0])
                elif 'mapped' in line and 'QC-passed' in line:
                    stats["mapped_reads"] = int(line.split()[0])
                elif 'properly paired' in line:
                    stats["properly_paired"] = int(line.split()[0])
            
            if stats["total_reads"] > 0:
                stats["mapping_rate"] = (stats["mapped_reads"] / stats["total_reads"]) * 100
                stats["proper_pair_rate"] = (stats["properly_paired"] / stats["total_reads"]) * 100
        
        # 통계 파일 저장
        stats_file = PyToolsConfig.RESULTS_DIR / f"{sample_id}_minimap2_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Minimap2 정렬 완료: {sample_id}")
        logger.info(f"  - 총 읽기 수: {stats['total_reads']:,}")
        logger.info(f"  - 매핑된 읽기 수: {stats['mapped_reads']:,}")
        logger.info(f"  - 적절한 쌍 수: {stats['properly_paired']:,}")
        logger.info(f"  - 매핑률: {stats['mapping_rate']:.2f}%")
        logger.info(f"  - 적절한 쌍 비율: {stats['proper_pair_rate']:.2f}%")
        logger.info(f"  - 매핑 시간: {stats['minimap2_time']:.2f}초")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "sam_file": str(sam_file),
            "bam_file": str(bam_file),
            "sorted_bam_file": str(sorted_bam_file),
            "stats_file": str(stats_file),
            "stats": stats,
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Minimap2 정렬 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "sam_file": None,
            "bam_file": None,
            "sorted_bam_file": None,
            "stats_file": None,
            "stats": None,
            "error": str(e)
        }
    finally:
        # 임시 파일 정리
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

class PyToolsAligner:
    """Minimap2를 사용한 정렬 클래스"""
    
    def __init__(self, spark: SparkSession, reference_genome: Path = None):
        self.spark = spark
        self.reference_genome = reference_genome or PyToolsConfig.REFERENCE_GENOME
        self.temp_files = []
        
        # 참조 게놈 확인
        if not self.reference_genome.exists():
            raise FileNotFoundError(f"참조 게놈 파일을 찾을 수 없습니다: {self.reference_genome}")
    
    def align_reads(self, preprocessed_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        """
        전처리된 읽기들을 참조 게놈에 매핑합니다.
        
        Args:
            preprocessed_df: 전처리 결과 DataFrame
        
        Returns:
            매핑 결과 DataFrame
        """
        logger.info("Minimap2 읽기 매핑 시작")
        
        # 성공한 전처리 결과만 필터링
        successful_df = preprocessed_df.filter(
            col("fastp_result.status") == "success"
        )
        
        if successful_df.count() == 0:
            logger.warning("매핑할 전처리된 파일이 없습니다.")
            return self.spark.createDataFrame([], StructType([
                StructField("sample_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("sam_file", StringType(), True),
                StructField("bam_file", StringType(), True),
                StructField("sorted_bam_file", StringType(), True),
                StructField("stats_file", StringType(), True),
                StructField("stats", StringType(), True),
                StructField("error", StringType(), True)
            ]))
        
        # UDF 등록
        minimap2_alignment_udf = udf(run_minimap2_alignment_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("sam_file", StringType(), True),
            StructField("bam_file", StringType(), True),
            StructField("sorted_bam_file", StringType(), True),
            StructField("stats_file", StringType(), True),
            StructField("stats", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # Minimap2 정렬 실행
        result_df = successful_df.withColumn(
            "alignment_result",
            minimap2_alignment_udf(
                col("fastp_result.sample_id"),
                col("fastp_result.r1_file"),
                col("fastp_result.r2_file"),
                lit(str(self.reference_genome))
            )
        )
        
        logger.info("Minimap2 읽기 매핑 완료")
        return result_df

def run_alignment(spark: SparkSession, preprocessed_df: "pyspark.sql.DataFrame",
                 reference_genome: Path = None) -> "pyspark.sql.DataFrame":
    """
    Minimap2 정렬을 실행합니다.
    
    Args:
        spark: SparkSession
        preprocessed_df: 전처리 결과 DataFrame
        reference_genome: 참조 게놈 파일
    
    Returns:
        정렬 결과 DataFrame
    """
    aligner = PyToolsAligner(spark, reference_genome)
    return aligner.align_reads(preprocessed_df)
