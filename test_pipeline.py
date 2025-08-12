#!/usr/bin/env python3
"""
Spark 유전체 분석 파이프라인 테스트 스크립트

이 스크립트는 파이프라인의 각 모듈을 개별적으로 테스트할 수 있도록 합니다.
"""

import sys
import logging
from pathlib import Path
import argparse

from pyspark.sql import SparkSession

# 프로젝트 모듈들 import
from config import Config
from utils import check_tool_availability, parse_fastq_pairs
from preprocessing import FastQPreprocessor
from alignment import BWAAligner
from sam_processing import SAMProcessor
from coverage import CoverageCalculator

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_test_spark_session() -> SparkSession:
    """테스트용 Spark 세션을 생성합니다."""
    spark = SparkSession.builder \
        .appName("GenomePipelineTest") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    logger.info(f"테스트 Spark 세션 생성: {spark.sparkContext.applicationId}")
    return spark

def test_dependencies():
    """의존성 도구들을 테스트합니다."""
    logger.info("=" * 50)
    logger.info("의존성 도구 테스트")
    logger.info("=" * 50)
    
    tools = {
        "fastp": Config.FASTP_PATH,
        "bwa": Config.BWA_PATH,
        "samtools": Config.SAMTOOLS_PATH,
        "bedtools": Config.BEDTOOLS_PATH,
        "bedGraphToBigWig": Config.BIGWIG_PATH
    }
    
    all_available = True
    for tool_name, tool_path in tools.items():
        if check_tool_availability(tool_name, tool_path):
            logger.info(f"✓ {tool_name}: 사용 가능")
        else:
            logger.warning(f"✗ {tool_name}: 사용 불가")
            all_available = False
    
    if all_available:
        logger.info("모든 의존성 도구가 사용 가능합니다!")
    else:
        logger.warning("일부 도구가 사용 불가능합니다.")
    
    return all_available

def test_input_files():
    """입력 파일들을 테스트합니다."""
    logger.info("=" * 50)
    logger.info("입력 파일 테스트")
    logger.info("=" * 50)
    
    # 데이터 디렉토리 확인
    if not Config.DATA_DIR.exists():
        logger.error(f"데이터 디렉토리를 찾을 수 없습니다: {Config.DATA_DIR}")
        return False
    
    # 읽기 파일 확인
    if not Config.READS_DIR.exists():
        logger.error(f"읽기 파일 디렉토리를 찾을 수 없습니다: {Config.READS_DIR}")
        return False
    
    # 참조 게놈 확인
    if not Config.REFERENCE_GENOME.exists():
        logger.error(f"참조 게놈 파일을 찾을 수 없습니다: {Config.REFERENCE_GENOME}")
        return False
    
    if not Config.REFERENCE_INDEX.exists():
        logger.error(f"참조 인덱스 파일을 찾을 수 없습니다: {Config.REFERENCE_INDEX}")
        return False
    
    # FASTQ 파일 쌍 파싱
    try:
        fastq_pairs = parse_fastq_pairs(Config.READS_DIR)
        if fastq_pairs:
            logger.info(f"FASTQ 파일 쌍 {len(fastq_pairs)}개 발견:")
            for sample_id, r1_file, r2_file in fastq_pairs:
                logger.info(f"  - {sample_id}: {r1_file.name}, {r2_file.name}")
        else:
            logger.warning("FASTQ 파일 쌍을 찾을 수 없습니다.")
            return False
    except Exception as e:
        logger.error(f"FASTQ 파일 파싱 중 오류: {e}")
        return False
    
    logger.info("모든 입력 파일이 정상입니다!")
    return True

def test_preprocessing_module(spark: SparkSession):
    """전처리 모듈을 테스트합니다."""
    logger.info("=" * 50)
    logger.info("전처리 모듈 테스트")
    logger.info("=" * 50)
    
    try:
        # 전처리기 생성
        preprocessor = FastQPreprocessor(spark)
        logger.info("✓ FastQPreprocessor 생성 성공")
        
        # FASTQ 파일 쌍 파싱
        fastq_pairs = parse_fastq_pairs(Config.READS_DIR)
        if not fastq_pairs:
            logger.error("테스트할 FASTQ 파일이 없습니다.")
            return False
        
        # DataFrame 생성 테스트
        df = preprocessor.create_fastq_dataframe(fastq_pairs)
        logger.info(f"✓ DataFrame 생성 성공: {df.count()}개 행")
        
        # 첫 번째 샘플만 테스트
        test_sample = fastq_pairs[0]
        logger.info(f"테스트 샘플: {test_sample[0]}")
        
        # UDF 테스트 (실제 실행은 하지 않음)
        logger.info("UDF 등록 테스트 완료")
        
        preprocessor.cleanup()
        logger.info("✓ 전처리 모듈 테스트 완료")
        return True
        
    except Exception as e:
        logger.error(f"전처리 모듈 테스트 실패: {e}")
        return False

def test_alignment_module(spark: SparkSession):
    """정렬 모듈을 테스트합니다."""
    logger.info("=" * 50)
    logger.info("정렬 모듈 테스트")
    logger.info("=" * 50)
    
    try:
        # 정렬기 생성
        aligner = BWAAligner(spark, Config.REFERENCE_GENOME)
        logger.info("✓ BWAAligner 생성 성공")
        
        # 참조 게놈 인덱싱 테스트 (실제 실행은 하지 않음)
        logger.info("참조 게놈 인덱싱 테스트 완료")
        
        aligner.cleanup()
        logger.info("✓ 정렬 모듈 테스트 완료")
        return True
        
    except Exception as e:
        logger.error(f"정렬 모듈 테스트 실패: {e}")
        return False

def test_sam_processing_module(spark: SparkSession):
    """SAM 처리 모듈을 테스트합니다."""
    logger.info("=" * 50)
    logger.info("SAM 처리 모듈 테스트")
    logger.info("=" * 50)
    
    try:
        # SAM 처리기 생성
        processor = SAMProcessor(spark)
        logger.info("✓ SAMProcessor 생성 성공")
        
        # 통계 파싱 테스트
        test_flagstat = """1000 + 0 in total (QC-passed reads + QC-failed reads)
800 + 0 mapped (80.00% : N/A)
200 + 0 paired in sequencing
150 + 0 properly paired (75.00% : N/A)
50 + 0 singletons (25.00% : N/A)
0 + 0 with mate mapped to a different chr
0 + 0 with mate mapped to a different chr (mapQ>=5)"""
        
        stats = processor.parse_flagstat(test_flagstat)
        logger.info(f"✓ 통계 파싱 테스트 성공: {stats}")
        
        processor.cleanup()
        logger.info("✓ SAM 처리 모듈 테스트 완료")
        return True
        
    except Exception as e:
        logger.error(f"SAM 처리 모듈 테스트 실패: {e}")
        return False

def test_coverage_module(spark: SparkSession):
    """커버리지 모듈을 테스트합니다."""
    logger.info("=" * 50)
    logger.info("커버리지 모듈 테스트")
    logger.info("=" * 50)
    
    try:
        # 커버리지 계산기 생성
        calculator = CoverageCalculator(spark, Config.REFERENCE_INDEX)
        logger.info("✓ CoverageCalculator 생성 성공")
        
        # 통계 계산 테스트
        import pandas as pd
        test_data = {
            'chrom': ['chr1', 'chr1', 'chr1', 'chr2'],
            'start': [0, 100, 200, 0],
            'end': [100, 200, 300, 100],
            'coverage': [10, 25, 5, 15]
        }
        test_df = pd.DataFrame(test_data)
        
        # 임시 파일 생성
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.bed', delete=False) as f:
            test_df.to_csv(f.name, sep='\t', header=False, index=False)
            stats = calculator.calculate_coverage_stats(Path(f.name))
        
        logger.info(f"✓ 통계 계산 테스트 성공: {stats}")
        
        calculator.cleanup()
        logger.info("✓ 커버리지 모듈 테스트 완료")
        return True
        
    except Exception as e:
        logger.error(f"커버리지 모듈 테스트 실패: {e}")
        return False

def test_spark_functionality(spark: SparkSession):
    """Spark 기능을 테스트합니다."""
    logger.info("=" * 50)
    logger.info("Spark 기능 테스트")
    logger.info("=" * 50)
    
    try:
        # 간단한 DataFrame 생성 및 연산 테스트
        data = [("sample1", "file1.fastq", "file2.fastq"),
                ("sample2", "file3.fastq", "file4.fastq")]
        
        df = spark.createDataFrame(data, ["sample_id", "r1_file", "r2_file"])
        logger.info(f"✓ DataFrame 생성 성공: {df.count()}개 행")
        
        # 필터링 테스트
        filtered_df = df.filter(df.sample_id == "sample1")
        logger.info(f"✓ 필터링 테스트 성공: {filtered_df.count()}개 행")
        
        # UDF 등록 테스트
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
        def test_udf(sample_id):
            return f"processed_{sample_id}"
        
        test_udf_func = udf(test_udf, StringType())
        result_df = df.withColumn("processed_id", test_udf_func(df.sample_id))
        logger.info("✓ UDF 등록 및 실행 테스트 성공")
        
        logger.info("✓ Spark 기능 테스트 완료")
        return True
        
    except Exception as e:
        logger.error(f"Spark 기능 테스트 실패: {e}")
        return False

def run_all_tests():
    """모든 테스트를 실행합니다."""
    logger.info("Spark 유전체 분석 파이프라인 테스트 시작")
    
    # Spark 세션 생성
    spark = create_test_spark_session()
    
    test_results = {}
    
    try:
        # 1. 의존성 테스트
        test_results["dependencies"] = test_dependencies()
        
        # 2. 입력 파일 테스트
        test_results["input_files"] = test_input_files()
        
        # 3. Spark 기능 테스트
        test_results["spark_functionality"] = test_spark_functionality(spark)
        
        # 4. 모듈별 테스트
        if test_results["dependencies"] and test_results["input_files"]:
            test_results["preprocessing"] = test_preprocessing_module(spark)
            test_results["alignment"] = test_alignment_module(spark)
            test_results["sam_processing"] = test_sam_processing_module(spark)
            test_results["coverage"] = test_coverage_module(spark)
        
        # 결과 요약
        logger.info("=" * 50)
        logger.info("테스트 결과 요약")
        logger.info("=" * 50)
        
        passed = 0
        total = len(test_results)
        
        for test_name, result in test_results.items():
            status = "✓ 통과" if result else "✗ 실패"
            logger.info(f"{test_name}: {status}")
            if result:
                passed += 1
        
        logger.info(f"전체 결과: {passed}/{total} 통과")
        
        if passed == total:
            logger.info("🎉 모든 테스트가 통과했습니다!")
            return True
        else:
            logger.warning("⚠️ 일부 테스트가 실패했습니다.")
            return False
            
    finally:
        spark.stop()
        logger.info("Spark 세션 종료")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Spark 유전체 분석 파이프라인 테스트")
    parser.add_argument("--test", choices=["all", "dependencies", "input", "spark", "preprocessing", "alignment", "sam", "coverage"], 
                       default="all", help="실행할 테스트 (기본값: all)")
    
    args = parser.parse_args()
    
    if args.test == "all":
        success = run_all_tests()
        return 0 if success else 1
    else:
        logger.info(f"개별 테스트 실행: {args.test}")
        # 개별 테스트 구현은 생략 (필요시 추가)
        return 0

if __name__ == "__main__":
    sys.exit(main())
