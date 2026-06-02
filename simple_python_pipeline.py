#!/usr/bin/env python3
"""
순수 Python으로 구현한 유전체 분석 파이프라인 (Spark 없이)
"""

import sys
import time
import logging
from pathlib import Path
from datetime import datetime
import json
import argparse
import subprocess
import tempfile
import shutil
import os

# 프로젝트 모듈들 import
from config_hybrid import HybridConfig as Config
from utils import check_tool_availability, parse_fastq_pairs
from pythontools.hybrid_sam_processing import run_pysam_processing_udf
from pythontools.hybrid_coverage import run_pybedtools_coverage_udf

# 필요한 디렉토리 생성
Config.create_directories()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.RESULTS_DIR / "simple_pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_fastp_simple(sample_id: str, r1_file: str, r2_file: str) -> dict:
    """fastp를 실행하는 함수 (Spark UDF 없이)"""
    temp_files = []
    try:
        # 임시 출력 파일 생성
        output_r1 = tempfile.NamedTemporaryFile(suffix=".trimmed.fastq", prefix=f"{sample_id}_R1_", delete=False)
        output_r2 = tempfile.NamedTemporaryFile(suffix=".trimmed.fastq", prefix=f"{sample_id}_R2_", delete=False)
        report_json = tempfile.NamedTemporaryFile(suffix=".json", prefix=f"{sample_id}_fastp_", delete=False)
        report_html = tempfile.NamedTemporaryFile(suffix=".html", prefix=f"{sample_id}_fastp_", delete=False)
        
        temp_files.extend([output_r1.name, output_r2.name, report_json.name, report_html.name])
        
        # fastp 명령어 실행
        cmd = [
            Config.FASTP_PATH,
            "-i", r1_file,
            "-I", r2_file,
            "-o", output_r1.name,
            "-O", output_r2.name,
            "--detect_adapter_for_pe",
            "--json", report_json.name,
            "--html", report_html.name,
            "--thread", "1"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            # 결과 파일들을 결과 디렉토리로 복사
            final_r1 = Config.RESULTS_DIR / f"{sample_id}.R1.trimmed.fastq"
            final_r2 = Config.RESULTS_DIR / f"{sample_id}.R2.trimmed.fastq"
            final_json = Config.RESULTS_DIR / f"report_{sample_id}_fastp.json"
            final_html = Config.RESULTS_DIR / f"report_{sample_id}_fastp.html"
            
            shutil.copy2(output_r1.name, final_r1)
            shutil.copy2(output_r2.name, final_r2)
            shutil.copy2(report_json.name, final_json)
            shutil.copy2(report_html.name, final_html)
            
            logger.info(f"fastp 처리 완료: {sample_id}")
            
            return {
                "sample_id": sample_id,
                "status": "success",
                "trimmed_r1": str(final_r1),
                "trimmed_r2": str(final_r2),
                "report_json": str(final_json),
                "report_html": str(final_html),
                "error": None
            }
        else:
            logger.error(f"fastp 처리 실패: {sample_id} - {result.stderr}")
            return {
                "sample_id": sample_id,
                "status": "failed",
                "trimmed_r1": None,
                "trimmed_r2": None,
                "report_json": None,
                "report_html": None,
                "error": result.stderr
            }
            
    except Exception as e:
        logger.error(f"fastp 처리 중 예외 발생: {sample_id} - {str(e)}")
        return {
            "sample_id": sample_id,
            "status": "error",
            "trimmed_r1": None,
            "trimmed_r2": None,
            "report_json": None,
            "report_html": None,
            "error": str(e)
        }
    finally:
        # 임시 파일 정리
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

def run_bwa_simple(sample_id: str, r1_file: str, r2_file: str, reference_genome: str) -> dict:
    """BWA를 실행하는 함수 (Spark UDF 없이)"""
    temp_files = []
    try:
        # 임시 SAM 파일 생성
        sam_file = tempfile.NamedTemporaryFile(suffix=".sam", prefix=f"{sample_id}_", delete=False)
        temp_files.append(sam_file.name)
        
        # BWA mem 명령어 실행
        cmd = [
            Config.BWA_PATH, "mem",
            "-t", "16",
            reference_genome,
            r1_file,
            r2_file
        ]
        
        with open(sam_file.name, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            # 결과 파일을 결과 디렉토리로 복사
            final_sam = Config.RESULTS_DIR / f"{sample_id}.sam"
            shutil.copy2(sam_file.name, final_sam)
            
            logger.info(f"BWA mem 처리 완료: {sample_id}")
            
            return {
                "sample_id": sample_id,
                "status": "success",
                "sam_file": str(final_sam),
                "error": None
            }
        else:
            logger.error(f"BWA mem 처리 실패: {sample_id} - {result.stderr}")
            return {
                "sample_id": sample_id,
                "status": "failed",
                "sam_file": None,
                "error": result.stderr
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
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

def run_simple_pipeline(reads_dir: Path, reference_genome: Path, reference_index: Path):
    """순수 Python으로 파이프라인 실행"""
    start_time = datetime.now()
    results = {}
    
    logger.info("=" * 80)
    logger.info("순수 Python 유전체 분석 파이프라인 시작")
    logger.info("=" * 80)
    logger.info("사용 기술:")
    logger.info("  - 전처리: fastp (외부 도구)")
    logger.info("  - 매핑: BWA (외부 도구)")
    logger.info("  - SAM 처리: pysam (Python 라이브러리)")
    logger.info("  - 커버리지: pybedtools + pybigwig (Python 라이브러리)")
    logger.info("=" * 80)
    
    try:
        # 입력 파일 확인
        fastq_pairs = parse_fastq_pairs(reads_dir)
        logger.info(f"처리할 FASTQ 파일 쌍: {len(fastq_pairs)}개")
        
        # 1단계: 전처리 (fastp) - 순차 처리
        logger.info("\n" + "=" * 50)
        logger.info("1단계: FASTQ 전처리 (fastp) - 순차 처리")
        logger.info("=" * 50)
        
        step1_start = time.time()
        preprocessing_results = []
        
        for sample_id, r1_file, r2_file in fastq_pairs:
            logger.info(f"전처리 시작: {sample_id}")
            result = run_fastp_simple(sample_id, str(r1_file), str(r2_file))
            preprocessing_results.append(result)
            logger.info(f"전처리 완료: {sample_id}")
        
        step1_time = time.time() - step1_start
        success_count = sum(1 for r in preprocessing_results if r["status"] == "success")
        
        results["preprocessing"] = {
            "status": "completed",
            "success_count": success_count,
            "total_count": len(preprocessing_results),
            "execution_time": step1_time
        }
        
        logger.info(f"전처리 완료: {success_count}/{len(preprocessing_results)} 성공 ({step1_time:.2f}초)")
        
        if success_count == 0:
            logger.error("전처리된 파일이 없어 파이프라인을 중단합니다.")
            return results
        
        # 2단계: 읽기 매핑 (BWA)
        logger.info("\n" + "=" * 50)
        logger.info("2단계: 읽기 매핑 (BWA)")
        logger.info("=" * 50)
        
        step2_start = time.time()
        alignment_results = []
        
        for result in preprocessing_results:
            if result["status"] == "success":
                bwa_result = run_bwa_simple(
                    result["sample_id"],
                    result["trimmed_r1"],
                    result["trimmed_r2"],
                    str(reference_genome)
                )
                alignment_results.append(bwa_result)
        
        step2_time = time.time() - step2_start
        success_count = sum(1 for r in alignment_results if r["status"] == "success")
        
        results["alignment"] = {
            "status": "completed",
            "success_count": success_count,
            "total_count": len(alignment_results),
            "execution_time": step2_time
        }
        
        logger.info(f"매핑 완료: {success_count}/{len(alignment_results)} 성공 ({step2_time:.2f}초)")
        
        if success_count == 0:
            logger.error("매핑된 파일이 없어 파이프라인을 중단합니다.")
            return results
        
        # 3단계: SAM 처리 (pysam)
        logger.info("\n" + "=" * 50)
        logger.info("3단계: SAM 처리 (pysam)")
        logger.info("=" * 50)
        
        step3_start = time.time()
        sam_processing_results = []
        
        for result in alignment_results:
            if result["status"] == "success":
                sam_result = run_pysam_processing_udf(
                    result["sample_id"],
                    result["sam_file"]
                )
                sam_processing_results.append(sam_result)
        
        step3_time = time.time() - step3_start
        success_count = sum(1 for r in sam_processing_results if r["status"] == "success")
        
        results["sam_processing"] = {
            "status": "completed",
            "success_count": success_count,
            "total_count": len(sam_processing_results),
            "execution_time": step3_time
        }
        
        logger.info(f"SAM 처리 완료: {success_count}/{len(sam_processing_results)} 성공 ({step3_time:.2f}초)")
        
        if success_count == 0:
            logger.error("처리된 BAM 파일이 없어 파이프라인을 중단합니다.")
            return results
        
        # 4단계: 커버리지 계산 (pybedtools + pybigwig)
        logger.info("\n" + "=" * 50)
        logger.info("4단계: 커버리지 계산 (pybedtools + pybigwig)")
        logger.info("=" * 50)
        
        step4_start = time.time()
        coverage_results = []
        
        for result in sam_processing_results:
            if result["status"] == "success":
                coverage_result = run_pybedtools_coverage_udf(
                    result["sample_id"],
                    result["bam_file"],
                    str(reference_index)
                )
                coverage_results.append(coverage_result)
        
        step4_time = time.time() - step4_start
        success_count = sum(1 for r in coverage_results if r["status"] == "success")
        
        results["coverage"] = {
            "status": "completed",
            "success_count": success_count,
            "total_count": len(coverage_results),
            "execution_time": step4_time
        }
        
        logger.info(f"커버리지 계산 완료: {success_count}/{len(coverage_results)} 성공 ({step4_time:.2f}초)")
        
        # 파이프라인 완료
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        results["pipeline"] = {
            "status": "completed",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_execution_time": total_time,
            "pipeline_type": "simple_python"
        }
        
        logger.info("\n" + "=" * 80)
        logger.info("순수 Python 유전체 분석 파이프라인 완료")
        logger.info(f"총 실행 시간: {total_time:.2f}초")
        logger.info("=" * 80)
        
        return results
        
    except Exception as e:
        logger.error(f"파이프라인 실행 중 오류 발생: {str(e)}")
        return {"error": str(e)}

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="순수 Python 유전체 분석 파이프라인")
    parser.add_argument("--reads-dir", type=str, help="읽기 파일 디렉토리 경로")
    parser.add_argument("--reference-genome", type=str, help="참조 게놈 파일 경로")
    parser.add_argument("--reference-index", type=str, help="참조 인덱스 파일 경로")
    
    args = parser.parse_args()
    
    try:
        # 파이프라인 실행
        reads_dir = Path(args.reads_dir) if args.reads_dir else None
        reference_genome = Path(args.reference_genome) if args.reference_genome else None
        reference_index = Path(args.reference_index) if args.reference_index else None
        
        results = run_simple_pipeline(reads_dir, reference_genome, reference_index)
        
        # 결과 저장
        results_file = Config.RESULTS_DIR / "simple_pipeline_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"파이프라인 결과 저장: {results_file}")
        
        # 성공 메시지
        if results.get("pipeline", {}).get("status") == "completed":
            logger.info("순수 Python 파이프라인이 성공적으로 완료되었습니다!")
            return 0
        else:
            logger.error("순수 Python 파이프라인이 실패했습니다.")
            return 1
            
    except Exception as e:
        logger.error(f"순수 Python 파이프라인 실행 중 오류 발생: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())

