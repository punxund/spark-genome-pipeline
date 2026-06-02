#!/usr/bin/env python3
"""
Minimap2를 사용한 Spark 유전체 분석 파이프라인

이 파이프라인은 효율적인 외부 도구들을 사용합니다:
1. 전처리: Python 네이티브 품질 트리밍
2. 읽기 매핑: Minimap2
3. SAM 처리: Samtools
4. 커버리지 계산: pybigwig
"""

import sys
import time
import logging
from pathlib import Path
from datetime import datetime
import json
import argparse
import psutil
import threading
from collections import defaultdict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Python 네이티브 모듈들 import
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_pytools import PyToolsConfig
from utils import check_tool_availability, parse_fastq_pairs
from pytools_preprocessing import run_preprocessing
from pytools_alignment import run_alignment
from pytools_sam_processing import run_sam_processing
from pytools_coverage import run_coverage_calculation

logger = logging.getLogger(__name__)

class SystemResourceMonitor:
    """시스템 리소스 사용량을 모니터링하는 클래스"""
    
    def __init__(self, log_interval=5):
        self.log_interval = log_interval
        self.monitoring = False
        self.monitor_thread = None
        self.resource_log = []
        self.start_time = None
        
    def start_monitoring(self):
        """모니터링 시작"""
        self.monitoring = True
        self.start_time = time.time()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info("시스템 리소스 모니터링 시작")
        
    def stop_monitoring(self):
        """모니터링 중지"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("시스템 리소스 모니터링 중지")
        
    def _monitor_loop(self):
        """모니터링 루프"""
        while self.monitoring:
            try:
                # CPU 사용량
                cpu_percent = psutil.cpu_percent(interval=1)
                
                # 메모리 사용량
                memory = psutil.virtual_memory()
                memory_percent = memory.percent
                memory_used_gb = memory.used / (1024**3)
                memory_total_gb = memory.total / (1024**3)
                
                # 디스크 사용량
                disk = psutil.disk_usage('/')
                disk_percent = disk.percent
                
                # 현재 시간
                current_time = time.time()
                elapsed_time = current_time - self.start_time
                
                # 로그 기록
                resource_info = {
                    "timestamp": current_time,
                    "elapsed_time": elapsed_time,
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory_percent,
                    "memory_used_gb": memory_used_gb,
                    "memory_total_gb": memory_total_gb,
                    "disk_percent": disk_percent
                }
                
                self.resource_log.append(resource_info)
                
                # 로그 출력
                logger.info(f"리소스 사용량 - CPU: {cpu_percent:.1f}%, "
                          f"메모리: {memory_percent:.1f}% ({memory_used_gb:.1f}GB/{memory_total_gb:.1f}GB), "
                          f"디스크: {disk_percent:.1f}%")
                
                time.sleep(self.log_interval)
                
            except Exception as e:
                logger.error(f"리소스 모니터링 오류: {str(e)}")
                time.sleep(self.log_interval)
                
    def get_summary(self):
        """리소스 사용량 요약 반환"""
        if not self.resource_log:
            return {}
            
        cpu_values = [log["cpu_percent"] for log in self.resource_log]
        memory_values = [log["memory_percent"] for log in self.resource_log]
        memory_used_values = [log["memory_used_gb"] for log in self.resource_log]
        
        return {
            "monitoring_duration": self.resource_log[-1]["elapsed_time"] if self.resource_log else 0,
            "cpu": {
                "avg": sum(cpu_values) / len(cpu_values),
                "max": max(cpu_values),
                "min": min(cpu_values)
            },
            "memory": {
                "avg_percent": sum(memory_values) / len(memory_values),
                "max_percent": max(memory_values),
                "min_percent": min(memory_values),
                "avg_used_gb": sum(memory_used_values) / len(memory_used_values),
                "max_used_gb": max(memory_used_values)
            },
            "total_samples": len(self.resource_log)
        }
        
    def save_log(self, file_path):
        """리소스 로그를 파일로 저장"""
        with open(file_path, 'w') as f:
            json.dump(self.resource_log, f, indent=2)
        logger.info(f"리소스 로그 저장: {file_path}")

def optimize_dataframe_for_parquet(df, step_name: str):
    """
    DataFrame을 Parquet 저장에 최적화합니다.
    
    Args:
        df: 최적화할 DataFrame
        step_name: 단계 이름 (로깅용)
    
    Returns:
        최적화된 DataFrame
    """
    logger.info(f"{step_name} - DataFrame 최적화 시작")
    
    # 기본 파티션 수 사용 (count() 호출 제거로 성능 향상)
    num_partitions = 8  # 고정된 적절한 파티션 수
    
    logger.info(f"{step_name} - 파티션 수: {num_partitions}")
    
    # 파티션 수 조정 (count() 없이)
    optimized_df = df.coalesce(num_partitions)
    
    # 캐시를 사용하여 반복 계산 방지
    optimized_df.cache()
    
    logger.info(f"{step_name} - DataFrame 최적화 완료")
    return optimized_df

def save_dataframe_as_parquet(df, file_path: Path, step_name: str):
    """
    DataFrame을 최적화된 설정으로 Parquet 파일로 저장합니다.
    
    Args:
        df: 저장할 DataFrame
        file_path: 저장할 파일 경로
        step_name: 단계 이름 (로깅용)
    """
    logger.info(f"{step_name} - Parquet 파일 저장 시작: {file_path}")
    
    # 간단한 Parquet 저장 설정
    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(str(file_path))
    
    # 캐시 해제
    df.unpersist()
    
    logger.info(f"{step_name} - Parquet 파일 저장 완료: {file_path}")

# 필요한 디렉토리 생성
PyToolsConfig.create_directories()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PyToolsConfig.RESULTS_DIR / "pytools_pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PyToolsGenomeAnalysisPipeline:
    """Python 네이티브 Spark 유전체 분석 파이프라인 클래스"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.start_time = None
        self.end_time = None
        self.results = {}
        self.resource_monitor = SystemResourceMonitor(log_interval=30)  # 30초마다 모니터링
        
        # 필요한 디렉토리 생성
        PyToolsConfig.create_directories()
        
        # Python 의존성 확인
        self._check_dependencies()
    
    def _check_dependencies(self):
        """필요한 Python 패키지들의 사용 가능 여부를 확인합니다."""
        logger.info("Python 의존성 확인 중...")
        
        if not PyToolsConfig.check_python_dependencies():
            logger.error("필요한 Python 패키지들이 설치되지 않았습니다.")
            logger.error("다음 명령어로 설치하세요:")
            logger.error("pip install pysam pybigwig numpy pandas biopython")
            sys.exit(1)
        
        logger.info("모든 Python 의존성이 사용 가능합니다.")
    
    def _check_input_files(self, reads_dir: Path = None) -> list:
        """입력 파일들을 확인합니다."""
        if reads_dir is None:
            reads_dir = PyToolsConfig.READS_DIR
        
        if not reads_dir.exists():
            raise FileNotFoundError(f"읽기 파일 디렉토리를 찾을 수 없습니다: {reads_dir}")
        
        # FASTQ 파일 쌍 파싱
        fastq_pairs = parse_fastq_pairs(reads_dir)
        
        if not fastq_pairs:
            raise ValueError(f"FASTQ 파일 쌍을 찾을 수 없습니다: {reads_dir}")
        
        logger.info(f"처리할 FASTQ 파일 쌍: {len(fastq_pairs)}개")
        for sample_id, r1_file, r2_file in fastq_pairs:
            logger.info(f"  - {sample_id}: {r1_file.name}, {r2_file.name}")
        
        return fastq_pairs
    
    def run_pipeline(self, reads_dir: Path = None, reference_genome: Path = None, 
                    reference_index: Path = None) -> dict:
        """
        전체 파이프라인을 실행합니다.
        
        Args:
            reads_dir: 읽기 파일 디렉토리
            reference_genome: 참조 게놈 파일
            reference_index: 참조 인덱스 파일
        
        Returns:
            파이프라인 실행 결과
        """
        self.start_time = datetime.now()
        logger.info("=" * 80)
        logger.info("Python 네이티브 Spark 유전체 분석 파이프라인 시작")
        logger.info("=" * 80)
        
        # 리소스 모니터링 시작
        self.resource_monitor.start_monitoring()
        
        try:
            # 입력 파일 확인
            fastq_pairs = self._check_input_files(reads_dir)
            
            # 1단계: Python 네이티브 전처리
            logger.info("\n" + "=" * 50)
            logger.info("1단계: Python 네이티브 FASTQ 전처리")
            logger.info("=" * 50)
            
            step1_start = time.time()
            preprocessed_df = run_preprocessing(self.spark, reads_dir)
            
            # 전처리 결과를 파일로 저장 (간단한 방식)
            preprocessed_file = PyToolsConfig.RESULTS_DIR / "preprocessed_results.parquet"
            logger.info("전처리 결과를 Parquet 파일로 저장 중...")
            preprocessed_df.write.mode("overwrite").parquet(str(preprocessed_file))
            logger.info(f"전처리 결과 저장 완료: {preprocessed_file}")
            
            step1_time = time.time() - step1_start
            
            success_count = preprocessed_df.filter(col("fastp_result.status") == "success").count()
            total_count = preprocessed_df.count()
            
            self.results["preprocessing"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step1_time
            }
            
            logger.info(f"Python 네이티브 전처리 완료: {success_count}/{total_count} 성공 ({step1_time:.2f}초)")
            logger.info(f"  - 성공률: {(success_count/total_count*100):.1f}%")
            logger.info(f"  - 처리 속도: {total_count/step1_time:.1f} reads/초")
            
            if success_count == 0:
                logger.error("전처리된 파일이 없어 파이프라인을 중단합니다.")
                return self.results
            
            # 2단계: Minimap2 읽기 매핑
            logger.info("\n" + "=" * 50)
            logger.info("2단계: Minimap2 읽기 매핑")
            logger.info("=" * 50)
            
            step2_start = time.time()
            # 저장된 전처리 결과를 읽어옴
            preprocessed_df = self.spark.read.parquet(str(preprocessed_file))
            alignment_df = run_alignment(self.spark, preprocessed_df, reference_genome)
            
            # 매핑 결과를 파일로 저장 (간단한 방식)
            alignment_file = PyToolsConfig.RESULTS_DIR / "alignment_results.parquet"
            logger.info("매핑 결과를 Parquet 파일로 저장 중...")
            alignment_df.write.mode("overwrite").parquet(str(alignment_file))
            logger.info(f"매핑 결과 저장 완료: {alignment_file}")
            
            step2_time = time.time() - step2_start
            
            success_count = alignment_df.filter(col("alignment_result.status") == "success").count()
            total_count = alignment_df.count()
            
            self.results["alignment"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step2_time
            }
            
            logger.info(f"Minimap2 매핑 완료: {success_count}/{total_count} 성공 ({step2_time:.2f}초)")
            logger.info(f"  - 성공률: {(success_count/total_count*100):.1f}%")
            logger.info(f"  - 처리 속도: {total_count/step2_time:.1f} reads/초")
            
            if success_count == 0:
                logger.error("매핑된 파일이 없어 파이프라인을 중단합니다.")
                return self.results
            
            # 3단계: Samtools SAM 처리
            logger.info("\n" + "=" * 50)
            logger.info("3단계: Samtools SAM 처리")
            logger.info("=" * 50)
            
            step3_start = time.time()
            # 저장된 매핑 결과를 읽어옴
            alignment_df = self.spark.read.parquet(str(alignment_file))
            sam_processed_df = run_sam_processing(self.spark, alignment_df)
            
            # SAM 처리 결과를 파일로 저장 (간단한 방식)
            sam_processed_file = PyToolsConfig.RESULTS_DIR / "sam_processed_results.parquet"
            logger.info("SAM 처리 결과를 Parquet 파일로 저장 중...")
            sam_processed_df.write.mode("overwrite").parquet(str(sam_processed_file))
            logger.info(f"SAM 처리 결과 저장 완료: {sam_processed_file}")
            
            step3_time = time.time() - step3_start
            
            success_count = sam_processed_df.filter(col("samtools_result.status") == "success").count()
            total_count = sam_processed_df.count()
            
            self.results["sam_processing"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step3_time
            }
            
            logger.info(f"Samtools SAM 처리 완료: {success_count}/{total_count} 성공 ({step3_time:.2f}초)")
            logger.info(f"  - 성공률: {(success_count/total_count*100):.1f}%")
            logger.info(f"  - 처리 속도: {total_count/step3_time:.1f} reads/초")
            
            if success_count == 0:
                logger.error("처리된 BAM 파일이 없어 파이프라인을 중단합니다.")
                return self.results
            
            # 4단계: Python 네이티브 커버리지 계산 (pybedtools + pybigwig)
            logger.info("\n" + "=" * 50)
            logger.info("4단계: Python 네이티브 커버리지 계산 (pybedtools + pybigwig)")
            logger.info("=" * 50)
            
            step4_start = time.time()
            # 저장된 SAM 처리 결과를 읽어옴
            sam_processed_df = self.spark.read.parquet(str(sam_processed_file))
            coverage_df = run_coverage_calculation(self.spark, sam_processed_df, reference_index)
            
            # 커버리지 결과를 파일로 저장 (간단한 방식)
            coverage_file = PyToolsConfig.RESULTS_DIR / "coverage_results.parquet"
            logger.info("커버리지 결과를 Parquet 파일로 저장 중...")
            coverage_df.write.mode("overwrite").parquet(str(coverage_file))
            logger.info(f"커버리지 결과 저장 완료: {coverage_file}")
            
            step4_time = time.time() - step4_start
            
            success_count = coverage_df.filter(col("coverage_result.status") == "success").count()
            total_count = coverage_df.count()
            
            self.results["coverage"] = {
                "status": "completed",
                "success_count": success_count,
                "total_count": total_count,
                "execution_time": step4_time
            }
            
            logger.info(f"Python 네이티브 커버리지 계산 완료: {success_count}/{total_count} 성공 ({step4_time:.2f}초)")
            logger.info(f"  - 성공률: {(success_count/total_count*100):.1f}%")
            logger.info(f"  - 처리 속도: {total_count/step4_time:.1f} reads/초")
            
            # 파이프라인 완료
            self.end_time = datetime.now()
            total_time = (self.end_time - self.start_time).total_seconds()
            
            # 리소스 모니터링 중지
            self.resource_monitor.stop_monitoring()
            
            # 리소스 사용량 요약 가져오기
            resource_summary = self.resource_monitor.get_summary()
            
            self.results["pipeline"] = {
                "status": "completed",
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat(),
                "total_execution_time": total_time,
                "resource_usage": resource_summary
            }
            
            logger.info("\n" + "=" * 80)
            logger.info("Minimap2 Spark 유전체 분석 파이프라인 완료")
            logger.info(f"총 실행 시간: {total_time:.2f}초")
            if resource_summary:
                logger.info(f"평균 CPU 사용량: {resource_summary['cpu']['avg']:.1f}%")
                logger.info(f"최대 CPU 사용량: {resource_summary['cpu']['max']:.1f}%")
                logger.info(f"평균 메모리 사용량: {resource_summary['memory']['avg_percent']:.1f}%")
                logger.info(f"최대 메모리 사용량: {resource_summary['memory']['max_percent']:.1f}%")
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
        results_file = PyToolsConfig.RESULTS_DIR / "pytools_pipeline_results.json"
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        logger.info(f"Minimap2 파이프라인 결과 저장: {results_file}")
        
        # 리소스 로그 저장
        resource_log_file = PyToolsConfig.RESULTS_DIR / "resource_usage_log.json"
        self.resource_monitor.save_log(resource_log_file)
        
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
        summary_file = PyToolsConfig.RESULTS_DIR / "pytools_pipeline_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Python 네이티브 요약 보고서 저장: {summary_file}")

def create_spark_session() -> SparkSession:
    """Spark 세션을 생성합니다."""
    spark_config = PyToolsConfig.get_spark_config()
    
    # Python 경로 설정 - 절대 경로로 수정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    python_path = f"{current_dir}:{parent_dir}"
    
    # 환경 변수로도 설정
    os.environ['PYTHONPATH'] = python_path
    
    spark = SparkSession.builder \
        .appName(spark_config["spark.app.name"]) \
        .master(spark_config["spark.master"]) \
        .config("spark.driver.memory", spark_config["spark.driver.memory"]) \
        .config("spark.executor.memory", spark_config["spark.executor.memory"]) \
        .config("spark.sql.adaptive.enabled", spark_config["spark.sql.adaptive.enabled"]) \
        .config("spark.sql.adaptive.coalescePartitions.enabled", spark_config["spark.sql.adaptive.coalescePartitions.enabled"]) \
        .config("spark.sql.adaptive.skewJoin.enabled", spark_config["spark.sql.adaptive.skewJoin.enabled"]) \
        .config("spark.serializer", spark_config["spark.serializer"]) \
        .config("spark.sql.adaptive.localShuffleReader.enabled", spark_config["spark.sql.adaptive.localShuffleReader.enabled"]) \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", spark_config["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"]) \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", spark_config["spark.sql.adaptive.skewJoin.skewedPartitionFactor"]) \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", spark_config["spark.sql.adaptive.advisoryPartitionSizeInBytes"]) \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", spark_config["spark.sql.adaptive.coalescePartitions.minPartitionNum"]) \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", spark_config["spark.sql.adaptive.coalescePartitions.initialPartitionNum"]) \
        .config("spark.memory.fraction", spark_config["spark.memory.fraction"]) \
        .config("spark.memory.storageFraction", spark_config["spark.memory.storageFraction"]) \
        .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", spark_config["spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold"]) \
        .config("spark.sql.adaptive.forceApply", spark_config["spark.sql.adaptive.forceApply"]) \
        .config("spark.sql.adaptive.logLevel", spark_config["spark.sql.adaptive.logLevel"]) \
        .config("spark.python.worker.memory", spark_config["spark.python.worker.memory"]) \
        .config("spark.python.worker.python", spark_config["spark.python.worker.python"]) \
        .config("spark.sql.execution.arrow.pyspark.enabled", spark_config["spark.sql.execution.arrow.pyspark.enabled"]) \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", spark_config["spark.sql.execution.arrow.maxRecordsPerBatch"]) \
        .config("spark.default.parallelism", spark_config["spark.default.parallelism"]) \
        .config("spark.sql.shuffle.partitions", spark_config["spark.sql.shuffle.partitions"]) \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", spark_config["spark.sql.adaptive.coalescePartitions.initialPartitionNum"]) \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", spark_config["spark.sql.adaptive.advisoryPartitionSizeInBytes"]) \
        .config("spark.python.worker.reuse", spark_config["spark.python.worker.reuse"]) \
        .config("spark.sql.parquet.compression.codec", spark_config["spark.sql.parquet.compression.codec"]) \
        .config("spark.sql.parquet.enableVectorizedReader", spark_config["spark.sql.parquet.enableVectorizedReader"]) \
        .config("spark.sql.parquet.recordLevelFilter.enabled", spark_config["spark.sql.parquet.recordLevelFilter.enabled"]) \
        .config("spark.sql.parquet.columnarReaderBatchSize", spark_config["spark.sql.parquet.columnarReaderBatchSize"]) \
        .config("spark.sql.parquet.mergeSchema", spark_config["spark.sql.parquet.mergeSchema"]) \
        .config("spark.sql.parquet.filterPushdown", spark_config["spark.sql.parquet.filterPushdown"]) \
        .config("spark.sql.parquet.aggregatePushdown", spark_config["spark.sql.parquet.aggregatePushdown"]) \
        .config("spark.python.worker.pythonpath", python_path) \
        .getOrCreate()
    
    # Spark 로그 레벨 설정 (진행상황 표시 개선)
    spark.sparkContext.setLogLevel("INFO")
    
    # 진행상황 모니터링 설정
    spark.conf.set("spark.sql.adaptive.logLevel", "INFO")
    spark.conf.set("spark.sql.execution.adaptive.logLevel", "INFO")
    
    logger.info(f"Python 네이티브 Spark 세션 생성: {spark.sparkContext.applicationId}")
    return spark

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Python 네이티브 Spark 유전체 분석 파이프라인")
    parser.add_argument("--reads-dir", type=str, help="읽기 파일 디렉토리 경로")
    parser.add_argument("--reference-genome", type=str, help="참조 게놈 파일 경로")
    parser.add_argument("--reference-index", type=str, help="참조 인덱스 파일 경로")
    parser.add_argument("--spark-master", type=str, default="local[*]", help="Spark 마스터 URL")
    
    args = parser.parse_args()
    
    # Spark 마스터 설정 업데이트
    if args.spark_master:
        PyToolsConfig.SPARK_MASTER = args.spark_master
    
    # Spark 세션 생성
    spark = create_spark_session()
    
    try:
        # 파이프라인 실행
        pipeline = PyToolsGenomeAnalysisPipeline(spark)
        
        reads_dir = Path(args.reads_dir) if args.reads_dir else None
        reference_genome = Path(args.reference_genome) if args.reference_genome else None
        reference_index = Path(args.reference_index) if args.reference_index else None
        
        results = pipeline.run_pipeline(reads_dir, reference_genome, reference_index)
        
        # 결과 저장
        pipeline.save_results()
        
        # 성공 메시지
        if results["pipeline"]["status"] == "completed":
            logger.info("Python 네이티브 파이프라인이 성공적으로 완료되었습니다!")
            return 0
        else:
            logger.error("Python 네이티브 파이프라인이 실패했습니다.")
            return 1
            
    except Exception as e:
        logger.error(f"Python 네이티브 파이프라인 실행 중 오류 발생: {str(e)}")
        return 1
    
    finally:
        # Spark 세션 종료
        spark.stop()
        logger.info("Python 네이티브 Spark 세션 종료")

if __name__ == "__main__":
    sys.exit(main())
