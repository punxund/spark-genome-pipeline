#!/usr/bin/env python3
"""
Spark 유전체 분석 파이프라인 메인 실행 파일

이 파이프라인은 다음 단계들을 순차적으로 실행합니다:
1. 전처리 (fastp): FASTQ 파일 정리
2. 읽기 매핑 (BWA): 참조 게놈에 정렬
3. SAM 처리 (samtools): 정렬 및 통계 생성
4. 커버리지 계산 (bedtools): 게놈 커버리지 분석
"""

import sys
import time
import logging
import os
from pathlib import Path
from datetime import datetime
import json
import argparse
from typing import Optional, Union

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 프로젝트 모듈들 import
from config import Config
from utils import check_tool_availability, get_fastq_pairs_for_pipeline
from hdfs_path_utils import is_hdfs_uri
from preprocessing import run_preprocessing
from alignment import run_alignment
from sam_processing import run_sam_processing
from coverage import run_coverage_calculation

# 필요한 디렉토리 생성
Config.create_directories()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.RESULTS_DIR / "pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def _local_parquet_uri(subpath: str) -> str:
    """로컬 결과 경로를 file:// 로 고정(Spark 기본 FS가 HDFS일 때 상대 경로가 HDFS로 잡히는 것을 방지)."""
    return (Config.RESULTS_DIR / subpath).resolve().as_uri()


class GenomeAnalysisPipeline:
    """Spark 유전체 분석 파이프라인 클래스"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.start_time = None
        self.end_time = None
        self.results = {}
        
        # 필요한 디렉토리 생성
        Config.create_directories()
        
        # 도구 사용 가능 여부 확인
        self._check_dependencies()
    
    def _check_dependencies(self):
        """필요한 도구들의 사용 가능 여부를 확인합니다."""
        logger.info("의존성 도구 확인 중...")
        
        tools = {
            "fastp": Config.FASTP_PATH,
            "bwa": Config.BWA_PATH,
            "samtools": Config.SAMTOOLS_PATH,
            "bedtools": Config.BEDTOOLS_PATH,
            "bedGraphToBigWig": Config.BIGWIG_PATH
        }
        
        missing_tools = []
        for tool_name, tool_path in tools.items():
            if not check_tool_availability(tool_name, tool_path):
                missing_tools.append(tool_name)
        
        if missing_tools:
            logger.error(f"다음 도구들이 설치되지 않았습니다: {', '.join(missing_tools)}")
            logger.error("필요한 도구들을 설치한 후 다시 시도하세요.")
            sys.exit(1)
        
        logger.info("모든 의존성 도구가 사용 가능합니다.")
    
    def _check_input_files(
        self, reads_dir: Optional[Union[Path, str]] = None
    ) -> list:
        """입력 파일들을 확인합니다. 읽기 디렉터리는 로컬 Path 또는 HDFS( hdfs://... )."""
        rdir = reads_dir if reads_dir is not None else Config.READS_DIR
        is_hdfs = isinstance(rdir, str) and is_hdfs_uri(rdir)
        if not is_hdfs:
            p = Path(rdir) if not isinstance(rdir, Path) else rdir
            if not p.exists():
                raise FileNotFoundError(f"읽기 파일 디렉토리를 찾을 수 없습니다: {p}")
        
        fastq_pairs = get_fastq_pairs_for_pipeline(reads_dir)
        
        if not fastq_pairs:
            raise ValueError(f"FASTQ 파일 쌍을 찾을 수 없습니다: {rdir}")
        
        logger.info(f"처리할 FASTQ 파일 쌍: {len(fastq_pairs)}개")
        for sample_id, r1_file, r2_file in fastq_pairs:
            b1 = Path(r1_file).name if not is_hdfs_uri(str(r1_file)) else r1_file.rsplit("/", 1)[-1]
            b2 = Path(r2_file).name if not is_hdfs_uri(str(r2_file)) else r2_file.rsplit("/", 1)[-1]
            logger.info(f"  - {sample_id}: {b1}, {b2}")
        
        return fastq_pairs
    
    def run_pipeline(
        self,
        reads_dir: Optional[Union[Path, str]] = None,
        reference_genome: Optional[Union[Path, str]] = None,
        reference_index: Optional[Union[Path, str]] = None,
    ) -> dict:
        """
        전체 파이프라인을 실행합니다.
        
        Args:
            reads_dir: 읽기 디렉터리 (로컬 Path 또는 HDFS)
            reference_genome: 참조 FASTA (로컬 Path 또는 HDFS; 기본: Config.REFERENCE_GENOME)
            reference_index: 참조 .fai (로컬 Path 또는 HDFS; 기본: Config.REFERENCE_INDEX)
        
        Returns:
            파이프라인 실행 결과
        """
        self.start_time = datetime.now()
        logger.info("=" * 80)
        logger.info("Spark 유전체 분석 파이프라인 시작")
        logger.info("=" * 80)
        
        try:
            # 입력 파일 확인
            fastq_pairs = self._check_input_files(reads_dir)
            
            # 1단계: 전처리 (fastp)
            logger.info("\n" + "=" * 50)
            logger.info("1단계: FASTQ 전처리 (fastp)")
            logger.info("=" * 50)
            
            step1_start = time.time()
            preprocessed_df = run_preprocessing(self.spark, reads_dir)
            
            # 전처리 결과를 파일로 저장
            preprocessed_file = _local_parquet_uri("preprocessed_results.parquet")
            preprocessed_df.write.mode("overwrite").parquet(preprocessed_file)
            logger.info(f"전처리 결과 저장: {preprocessed_file}")
            
            step1_time = time.time() - step1_start
            
            success_count = preprocessed_df.filter(col("fastp_result.status") == "success").count()
            total_count = preprocessed_df.count()
            
            self.results["preprocessing"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step1_time
            }
            
            logger.info(f"전처리 완료: {success_count}/{total_count} 성공 ({step1_time:.2f}초)")
            
            if success_count == 0:
                logger.error("전처리된 파일이 없어 파이프라인을 중단합니다.")
                return self.results
            
            # 2단계: 읽기 매핑 (BWA)
            logger.info("\n" + "=" * 50)
            logger.info("2단계: 읽기 매핑 (BWA)")
            logger.info("=" * 50)
            
            step2_start = time.time()
            # 저장된 전처리 결과를 읽어옴
            preprocessed_df = self.spark.read.parquet(preprocessed_file)
            alignment_df = run_alignment(self.spark, preprocessed_df, reference_genome)
            
            # 매핑 결과를 파일로 저장
            alignment_file = _local_parquet_uri("alignment_results.parquet")
            alignment_df.write.mode("overwrite").parquet(alignment_file)
            logger.info(f"매핑 결과 저장: {alignment_file}")
            
            step2_time = time.time() - step2_start
            
            success_count = alignment_df.filter(col("bwa_result.status") == "success").count()
            total_count = alignment_df.count()
            
            self.results["alignment"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step2_time
            }
            
            logger.info(f"매핑 완료: {success_count}/{total_count} 성공 ({step2_time:.2f}초)")
            
            if success_count == 0:
                logger.error("매핑된 파일이 없어 파이프라인을 중단합니다.")
                return self.results
            
            # 3단계: SAM 처리 (samtools)
            logger.info("\n" + "=" * 50)
            logger.info("3단계: SAM 처리 (samtools)")
            logger.info("=" * 50)
            
            step3_start = time.time()
            # 저장된 매핑 결과를 읽어옴
            alignment_df = self.spark.read.parquet(alignment_file)
            sam_processed_df = run_sam_processing(self.spark, alignment_df)
            
            # SAM 처리 결과를 파일로 저장
            sam_processed_file = _local_parquet_uri("sam_processed_results.parquet")
            sam_processed_df.write.mode("overwrite").parquet(sam_processed_file)
            logger.info(f"SAM 처리 결과 저장: {sam_processed_file}")
            
            step3_time = time.time() - step3_start
            
            success_count = sam_processed_df.filter(col("samtools_result.status") == "success").count()
            total_count = sam_processed_df.count()
            
            self.results["sam_processing"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step3_time
            }
            
            logger.info(f"SAM 처리 완료: {success_count}/{total_count} 성공 ({step3_time:.2f}초)")
            
            if success_count == 0:
                logger.error("처리된 BAM 파일이 없어 파이프라인을 중단합니다.")
                return self.results
            
            # 4단계: 커버리지 계산 (bedtools)
            logger.info("\n" + "=" * 50)
            logger.info("4단계: 커버리지 계산 (bedtools)")
            logger.info("=" * 50)
            
            step4_start = time.time()
            # 저장된 SAM 처리 결과를 읽어옴
            sam_processed_df = self.spark.read.parquet(sam_processed_file)
            coverage_df = run_coverage_calculation(self.spark, sam_processed_df, reference_index)
            
            # 커버리지 결과를 파일로 저장
            coverage_file = _local_parquet_uri("coverage_results.parquet")
            coverage_df.write.mode("overwrite").parquet(coverage_file)
            logger.info(f"커버리지 결과 저장: {coverage_file}")
            
            step4_time = time.time() - step4_start
            
            success_count = coverage_df.filter(col("coverage_result.status") == "success").count()
            total_count = coverage_df.count()
            
            self.results["coverage"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step4_time
            }
            
            logger.info(f"커버리지 계산 완료: {success_count}/{total_count} 성공 ({step4_time:.2f}초)")
            
            # 파이프라인 완료
            self.end_time = datetime.now()
            total_time = (self.end_time - self.start_time).total_seconds()
            
            self.results["pipeline"] = {
                "status": "completed",
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat(),
                "total_execution_time": total_time
            }
            
            logger.info("\n" + "=" * 80)
            logger.info("Spark 유전체 분석 파이프라인 완료")
            logger.info(f"총 실행 시간: {total_time:.2f}초")
            logger.info("=" * 80)
            
            return self.results
            
        except Exception as e:
            logger.error(f"파이프라인 실행 중 오류 발생: {str(e)}")
            self.results["pipeline"] = {
                "status": "failed",
                "error": str(e),
                "start_time": self.start_time.isoformat() if self.start_time else None,
                "end_time": datetime.now().isoformat()
            }
            raise
    
    def save_results(self):
        """파이프라인 결과를 파일로 저장합니다."""
        if not self.results:
            logger.warning("저장할 결과가 없습니다.")
            return
        
        # 결과 파일 저장
        results_file = Config.RESULTS_DIR / "pipeline_results.json"
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        logger.info(f"파이프라인 결과 저장: {results_file}")
        
        # 요약 보고서 생성
        self._generate_summary_report()
    
    def _generate_summary_report(self):
        """요약 보고서를 생성합니다."""
        if "pipeline" not in self.results:
            return
        
        summary = {
            "pipeline_summary": {
                "status": self.results["pipeline"]["status"],
                "total_execution_time": self.results["pipeline"].get("total_execution_time", 0),
                "start_time": self.results["pipeline"].get("start_time"),
                "end_time": self.results["pipeline"].get("end_time")
            },
            "step_summaries": {}
        }
        
        for step_name, step_result in self.results.items():
            if step_name != "pipeline":
                summary["step_summaries"][step_name] = {
                    "status": step_result.get("status"),
                    "success_count": step_result.get("success_count", 0),
                    "total_count": step_result.get("total_count", 0),
                    "execution_time": step_result.get("execution_time", 0)
                }
        
        # 요약 보고서 저장
        summary_file = Config.RESULTS_DIR / "pipeline_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"요약 보고서 저장: {summary_file}")

def create_spark_session() -> SparkSession:
    """Spark 세션을 생성합니다."""
    spark_config = Config.get_spark_config()
    
    spark = SparkSession.builder \
        .appName(spark_config["spark.app.name"]) \
        .master(spark_config["spark.master"]) \
        .config("spark.driver.memory", spark_config["spark.driver.memory"]) \
        .config("spark.executor.memory", spark_config["spark.executor.memory"]) \
        .config("spark.sql.adaptive.enabled", spark_config["spark.sql.adaptive.enabled"]) \
        .config("spark.sql.adaptive.coalescePartitions.enabled", spark_config["spark.sql.adaptive.coalescePartitions.enabled"]) \
        .config("spark.sql.adaptive.skewJoin.enabled", spark_config["spark.sql.adaptive.skewJoin.enabled"]) \
        .getOrCreate()
    
    logger.info(f"Spark 세션 생성: {spark.sparkContext.applicationId}")
    return spark


def _as_pipeline_path(
    s: Optional[Union[Path, str]], fallback: Optional[Union[Path, str]] = None
) -> Optional[Union[Path, str]]:
    """HDFS URI는 str로 유지하고, 그 외는 Path로 맞춘다."""
    v = s if s is not None else fallback
    if v is None:
        return None
    if isinstance(v, str) and is_hdfs_uri(v):
        return v
    if isinstance(v, Path):
        return v
    return Path(v)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Spark 유전체 분석 파이프라인")
    parser.add_argument("--reads-dir", type=str, help="읽기 파일 디렉토리 경로")
    parser.add_argument("--reference-genome", type=str, help="참조 게놈 파일 경로")
    parser.add_argument("--reference-index", type=str, help="참조 인덱스 파일 경로")
    parser.add_argument("--spark-master", type=str, default="local[*]", help="Spark 마스터 URL")
    
    args = parser.parse_args()
    
    # Spark 마스터 설정 업데이트
    if args.spark_master:
        Config.SPARK_MASTER = args.spark_master

    if os.environ.get("SPARK_MAIN_SPARK_MASTER"):
        Config.SPARK_MASTER = os.environ["SPARK_MAIN_SPARK_MASTER"]
    
    # Spark 세션 생성
    spark = create_spark_session()
    
    try:
        # 파이프라인 실행
        pipeline = GenomeAnalysisPipeline(spark)
        
        reads_dir: Optional[Union[Path, str]] = None
        if args.reads_dir:
            reads_dir = args.reads_dir
        elif Config.MAIN_PIPELINE_HDFS_READS:
            reads_dir = Config.MAIN_PIPELINE_HDFS_READS
        if reads_dir is not None and not (isinstance(reads_dir, str) and is_hdfs_uri(reads_dir)):
            reads_dir = Path(reads_dir)
        
        ref_g = args.reference_genome
        if not ref_g and Config.MAIN_PIPELINE_HDFS_REF:
            ref_g = Config.MAIN_PIPELINE_HDFS_REF
        ref_g = _as_pipeline_path(ref_g, None)
        ref_i = args.reference_index
        if not ref_i and Config.MAIN_PIPELINE_HDFS_FAI:
            ref_i = Config.MAIN_PIPELINE_HDFS_FAI
        ref_i = _as_pipeline_path(ref_i, None)
        
        results = pipeline.run_pipeline(reads_dir, ref_g, ref_i)
        
        # 결과 저장
        pipeline.save_results()
        
        # 성공 메시지
        if results["pipeline"]["status"] == "completed":
            logger.info("파이프라인이 성공적으로 완료되었습니다!")
            return 0
        else:
            logger.error("파이프라인이 실패했습니다.")
            return 1
            
    except Exception as e:
        logger.error(f"파이프라인 실행 중 오류 발생: {str(e)}")
        return 1
    
    finally:
        # Spark 세션 종료
        spark.stop()
        logger.info("Spark 세션 종료")

if __name__ == "__main__":
    sys.exit(main())
