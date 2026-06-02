import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional

# PySpark import
try:
    from pyspark.sql import SparkSession
except ImportError as e:
    print(f"❌ PySpark import 오류: {e}")
    print("PySpark를 설치해주세요: pip install pyspark")
    sys.exit(1)

# 최적화된 설정 파일 import
try:
    from config_pytools_optimized import PyToolsConfig
except ImportError:
    print("❌ 최적화된 설정 파일을 찾을 수 없습니다.")
    print("config_pytools_optimized.py 파일이 필요합니다.")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """최적화된 Spark 세션을 생성합니다 - 핵심 설정만 사용"""
    spark_config = PyToolsConfig.get_spark_config()
    
    # Python 경로 설정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    python_path = f"{current_dir}:{parent_dir}"
    
    # 핵심 설정만 사용하여 Spark 세션 생성
    spark = SparkSession.builder \
        .appName(spark_config["spark.app.name"]) \
        .master(spark_config["spark.master"]) \
        .config("spark.driver.memory", spark_config["spark.driver.memory"]) \
        .config("spark.executor.memory", spark_config["spark.executor.memory"]) \
        .config("spark.memory.fraction", spark_config["spark.memory.fraction"]) \
        .config("spark.default.parallelism", spark_config["spark.default.parallelism"]) \
        .config("spark.sql.shuffle.partitions", spark_config["spark.sql.shuffle.partitions"]) \
        .config("spark.sql.adaptive.enabled", spark_config["spark.sql.adaptive.enabled"]) \
        .config("spark.serializer", spark_config["spark.serializer"]) \
        .config("spark.python.worker.memory", spark_config["spark.python.worker.memory"]) \
        .config("spark.python.worker.pythonpath", python_path) \
        .getOrCreate()
    
    # Spark 로그 레벨 설정
    spark.sparkContext.setLogLevel("INFO")
    
    logger.info(f"✅ 최적화된 Spark 세션 생성 완료: {spark.sparkContext.applicationId}")
    return spark

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="최적화된 Python 네이티브 Spark 유전체 분석 파이프라인")
    parser.add_argument("--reads-dir", type=str, help="읽기 파일 디렉토리 경로")
    parser.add_argument("--reference-genome", type=str, help="참조 게놈 파일 경로")
    parser.add_argument("--spark-master", type=str, default="local[*]", help="Spark 마스터 URL")
    
    args = parser.parse_args()
    
    try:
        # Spark 세션 생성
        spark = create_spark_session()
        
        # 설정 정보 출력
        logger.info("🚀 최적화된 파이프라인 설정:")
        logger.info(f"  - 최대 메모리: {PyToolsConfig.MAX_MEMORY}")
        logger.info(f"  - 최대 코어: {PyToolsConfig.MAX_CORES}")
        logger.info(f"  - 파티션 크기: {PyToolsConfig.PARTITION_SIZE:,}")
        
        # 성공 메시지
        logger.info("🎉 최적화된 파이프라인 초기화 완료!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 파이프라인 초기화 실패: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
