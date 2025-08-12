from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, StringType
import logging
from pathlib import Path
from typing import List, Tuple, Optional
import tempfile
import shutil
import os

from config import Config
from utils import run_command, check_tool_availability, create_temp_file, cleanup_temp_files

logger = logging.getLogger(__name__)

def run_bwa_mem_udf(sample_id: str, r1_file: str, r2_file: str, reference_genome: str) -> dict:
    """
    BWA mem을 실행하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        r1_file: R1 파일 경로
        r2_file: R2 파일 경로
        reference_genome: 참조 게놈 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    try:
        # 참조 게놈 인덱싱 확인 및 생성
        index_files = [f"{reference_genome}.amb", f"{reference_genome}.ann", f"{reference_genome}.bwt", f"{reference_genome}.pac", f"{reference_genome}.sa"]
        index_exists = all(Path(f).exists() for f in index_files)
        
        if not index_exists:
            logger.info(f"참조 게놈 인덱싱 시작: {reference_genome}")
            index_cmd = [Config.BWA_PATH, "index", reference_genome]
            index_result = run_command(index_cmd, check=False)
            
            if index_result.returncode != 0:
                logger.error(f"참조 게놈 인덱싱 실패: {index_result.stderr}")
                return {
                    "sample_id": sample_id,
                    "status": "failed",
                    "sam_file": None,
                    "error": f"참조 게놈 인덱싱 실패: {index_result.stderr}"
                }
            logger.info(f"참조 게놈 인덱싱 완료: {reference_genome}")
        
        # 임시 SAM 파일 생성
        sam_file = create_temp_file(suffix=".sam", prefix=f"{sample_id}_")
        temp_files.append(sam_file)
        
        # BWA mem 명령어 실행
        cmd = [
            Config.BWA_PATH, "mem",
            "-t", "1",  # 단일 스레드로 실행 (Spark에서 병렬 처리)
            reference_genome,
            r1_file,
            r2_file
        ]
        
        # SAM 출력을 파일로 리다이렉트
        with open(sam_file, 'w') as f:
            result = run_command(cmd, check=False)
            if result.returncode == 0:
                f.write(result.stdout)
            else:
                logger.error(f"BWA mem 실행 실패: {sample_id} - {result.stderr}")
                return {
                    "sample_id": sample_id,
                    "status": "failed",
                    "sam_file": None,
                    "error": result.stderr
                }
        
        # 결과 파일을 결과 디렉토리로 복사
        final_sam = Config.RESULTS_DIR / f"{sample_id}.sam"
        shutil.copy2(sam_file, final_sam)
        
        logger.info(f"BWA mem 처리 완료: {sample_id}")
        
        return {
            "sample_id": sample_id,
            "status": "success",
            "sam_file": str(final_sam),
            "error": None
        }
            
    except Exception as e:
        logger.error(f"BWA mem 처리 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "sam_file": None,
            "error": str(e)
        }
    finally:
        # 임시 파일 정리
        cleanup_temp_files(temp_files)

class BWAAligner:
    """Spark를 사용한 BWA 읽기 매핑 클래스"""
    
    def __init__(self, spark: SparkSession, reference_genome: Path = None):
        self.spark = spark
        self.temp_files = []
        self.reference_genome = reference_genome or Config.REFERENCE_GENOME
        self.reference_indexed = False
        
        # BWA 도구 사용 가능 여부 확인
        if not check_tool_availability("bwa", Config.BWA_PATH):
            raise RuntimeError("BWA 도구를 찾을 수 없습니다. 설치 후 다시 시도하세요.")
        
        # 참조 게놈 파일 존재 확인
        if not self.reference_genome.exists():
            raise FileNotFoundError(f"참조 게놈 파일을 찾을 수 없습니다: {self.reference_genome}")
    
    def index_reference_genome(self) -> bool:
        """
        참조 게놈을 인덱싱합니다.
        
        Returns:
            인덱싱 성공 여부
        """
        if self.reference_indexed:
            logger.info("참조 게놈이 이미 인덱싱되어 있습니다.")
            return True
        
        logger.info(f"참조 게놈 인덱싱 시작: {self.reference_genome}")
        
        try:
            # BWA 인덱스 생성
            cmd = [Config.BWA_PATH, "index", str(self.reference_genome)]
            result = run_command(cmd, check=False)
            
            if result.returncode == 0:
                self.reference_indexed = True
                logger.info("참조 게놈 인덱싱 완료")
                return True
            else:
                logger.error(f"참조 게놈 인덱싱 실패: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"참조 게놈 인덱싱 중 예외 발생: {str(e)}")
            return False
    
    def process_alignment(self, preprocessed_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
        """
        전처리된 FASTQ 파일들을 BWA로 매핑합니다.
        
        Args:
            preprocessed_df: 전처리 결과 DataFrame
        
        Returns:
            매핑 결과 DataFrame
        """
        logger.info("BWA 읽기 매핑 시작")
        
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
                StructField("error", StringType(), True)
            ]))
        
        # UDF 등록
        bwa_udf = udf(run_bwa_mem_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("sam_file", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # BWA mem 실행
        result_df = successful_df.withColumn(
            "bwa_result",
            bwa_udf(
                col("fastp_result.sample_id"),
                col("fastp_result.trimmed_r1"),
                col("fastp_result.trimmed_r2"),
                lit(str(self.reference_genome))
            )
        )
        
        # 결과 확인
        success_count = result_df.filter(col("bwa_result.status") == "success").count()
        total_count = result_df.count()
        
        logger.info(f"BWA 매핑 완료: {success_count}/{total_count} 성공")
        
        return result_df
    
    def cleanup(self):
        """임시 파일들을 정리합니다."""
        cleanup_temp_files(self.temp_files)
        self.temp_files.clear()

def run_alignment(spark: SparkSession, preprocessed_df: "pyspark.sql.DataFrame", 
                 reference_genome: Path = None) -> "pyspark.sql.DataFrame":
    """
    읽기 매핑 파이프라인을 실행합니다.
    
    Args:
        spark: SparkSession
        preprocessed_df: 전처리 결과 DataFrame
        reference_genome: 참조 게놈 파일 경로 (기본값: Config.REFERENCE_GENOME)
    
    Returns:
        매핑 결과 DataFrame
    """
    if reference_genome is None:
        reference_genome = Config.REFERENCE_GENOME
    
    # BWA 매핑 실행
    aligner = BWAAligner(spark, reference_genome)
    try:
        result_df = aligner.process_alignment(preprocessed_df)
        return result_df
    finally:
        aligner.cleanup()
