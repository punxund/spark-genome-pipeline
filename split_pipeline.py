#!/usr/bin/env python3
"""
분할된 데이터를 처리하는 파이프라인
올바른 방법: 각 청크를 독립적으로 처리 -> BAM 파일들 합치기 -> 커버리지 계산
"""

import logging
import time
import os
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple
import json

# Spark 관련 import
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType

# 설정 파일 import
from config_hybrid import HybridConfig as Config
from config_hybrid import HybridConfig as PyToolsConfig
from preprocessing import run_preprocessing
from alignment import run_alignment
from pythontools.hybrid_sam_processing import run_sam_processing
from pythontools.hybrid_coverage import run_coverage_calculation

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SplitDataPipeline:
    """분할된 데이터를 처리하는 파이프라인"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results = {}
        
        # 임시 디렉토리 설정
        os.environ['TMPDIR'] = str(Config.TEMP_DIR.absolute())
        os.environ['TEMP'] = str(Config.TEMP_DIR.absolute())
        os.environ['TMP'] = str(Config.TEMP_DIR.absolute())
    
    def _prepare_chunk_files(self, split_dir: Path) -> List[Tuple[str, str, str]]:
        """분할된 파일들을 청크 쌍으로 준비"""
        logger.info(f"분할된 파일 확인: {split_dir}")
        
        # R1, R2 청크 파일들 찾기
        r1_chunks = list(split_dir.glob("*_1_chunk*"))
        r2_chunks = list(split_dir.glob("*_2_chunk*"))
        
        logger.info(f"발견된 R1 청크 수: {len(r1_chunks)}")
        logger.info(f"발견된 R2 청크 수: {len(r2_chunks)}")
        
        # 청크 쌍 매칭
        chunk_pairs = []
        for r1_chunk in sorted(r1_chunks):
            # R1 청크에서 청크 번호 추출 (예: SRR30977596_1_chunk01 -> 01)
            chunk_num = r1_chunk.name.split('chunk')[-1]
            r2_chunk = split_dir / f"SRR30977596_2_chunk{chunk_num}"
            
            if r2_chunk.exists():
                chunk_id = f"chunk{chunk_num}"
                chunk_pairs.append((chunk_id, str(r1_chunk), str(r2_chunk)))
                logger.info(f"청크 쌍 발견: {chunk_id} -> {r1_chunk.name}, {r2_chunk.name}")
            else:
                logger.warning(f"R2 청크를 찾을 수 없음: {r2_chunk}")
        
        logger.info(f"총 {len(chunk_pairs)}개의 청크 쌍 준비 완료")
        return chunk_pairs
    
    def merge_bam_files(self, bam_files: List[str], output_bam: str) -> bool:
        """samtools merge를 사용하여 BAM 파일들 합치기"""
        try:
            logger.info(f"BAM 파일 합치기 시작: {len(bam_files)}개 파일 -> {output_bam}")
            
            # samtools merge 명령어 구성
            cmd = ["samtools", "merge", "-f", output_bam] + bam_files
            
            # 명령어 실행
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"BAM 파일 합치기 완료: {output_bam}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"BAM 파일 합치기 실패: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"BAM 파일 합치기 중 오류: {str(e)}")
            return False
    
    def run_pipeline(self, split_dir: Path, reference_genome: Path, 
                    reference_index: Path) -> dict:
        """분할된 데이터 파이프라인 실행"""
        start_time = datetime.now()
        logger.info("분할된 데이터 파이프라인 시작")
        
        # 결과 디렉토리 생성 (split_pipeline 전용)
        split_results_dir = Path("results/split_pipeline")
        split_results_dir.mkdir(parents=True, exist_ok=True)
        Config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
        
        # 0단계: 청크 파일들 준비
        logger.info("\n" + "=" * 50)
        logger.info("0단계: 청크 파일들 준비")
        logger.info("=" * 50)
        
        step0_start = time.time()
        chunk_pairs = self._prepare_chunk_files(split_dir)
        if not chunk_pairs:
            logger.error("처리할 청크 쌍이 없습니다.")
            return {"status": "error", "message": "처리할 청크 쌍이 없습니다."}
        
        step0_time = time.time() - step0_start
        logger.info(f"청크 쌍 준비 완료: {len(chunk_pairs)}개 ({step0_time:.2f}초)")
        
        # 1단계: 전처리 (fastp) - 각 청크별로 독립적으로 처리
        logger.info("\n" + "=" * 50)
        logger.info("1단계: 전처리 (fastp) - 각 청크 독립 처리")
        logger.info("=" * 50)
        
        step1_start = time.time()
        
        # DataFrame 생성 (R1, R2 쌍으로 처리)
        preprocessing_data = []
        for chunk_id, r1_file, r2_file in chunk_pairs:
            preprocessing_data.append({
                "sample_id": chunk_id,
                "r1_file": r1_file,
                "r2_file": r2_file
            })
        
        preprocessing_df = self.spark.createDataFrame(preprocessing_data)
        
        # 전처리 실행 (직접 처리)
        from preprocessing import FastQPreprocessor
        
        # chunk_pairs를 Path 객체로 변환
        fastq_pairs = []
        for chunk_id, r1_file, r2_file in chunk_pairs:
            fastq_pairs.append((chunk_id, Path(r1_file), Path(r2_file)))
        
        # FastQPreprocessor로 처리
        preprocessor = FastQPreprocessor(self.spark)
        try:
            preprocessing_df = preprocessor.process_fastq_files(fastq_pairs)
        finally:
            preprocessor.cleanup()
        
        # 전처리 결과 저장
        preprocessing_file = split_results_dir / "split_preprocessing_results.parquet"
        preprocessing_df.write.mode("overwrite").parquet(str(preprocessing_file))
        
        step1_time = time.time() - step1_start
        
        # DataFrame 액션을 한 번에 실행하여 중복 실행 방지
        preprocessing_results = preprocessing_df.collect()
        success_count = sum(1 for row in preprocessing_results if row["fastp_result"]["status"] == "success")
        total_count = len(preprocessing_results)
        
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
        
        # 2단계: 매핑 (BWA) - 각 청크별로 독립적으로 처리
        logger.info("\n" + "=" * 50)
        logger.info("2단계: 매핑 (BWA) - 각 청크 독립 처리")
        logger.info("=" * 50)
        
        step2_start = time.time()
        
        # 저장된 전처리 결과를 읽어옴
        preprocessing_df = self.spark.read.parquet(str(preprocessing_file))
        
        # 매핑 실행
        alignment_df = run_alignment(self.spark, preprocessing_df, reference_genome)
        
        # 매핑 결과 저장
        alignment_file = split_results_dir / "split_alignment_results.parquet"
        alignment_df.write.mode("overwrite").parquet(str(alignment_file))
        
        step2_time = time.time() - step2_start
        
        # DataFrame 액션을 한 번에 실행하여 중복 실행 방지
        alignment_results = alignment_df.collect()
        success_count = sum(1 for row in alignment_results if row["bwa_result"]["status"] == "success")
        total_count = len(alignment_results)
        
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
        
        # 3단계: SAM 처리 - 각 청크별로 독립적으로 처리
        logger.info("\n" + "=" * 50)
        logger.info("3단계: SAM 처리 - 각 청크 독립 처리")
        logger.info("=" * 50)
        
        step3_start = time.time()
        
        # 저장된 매핑 결과를 읽어옴
        alignment_df = self.spark.read.parquet(str(alignment_file))
        
        # SAM 처리 실행
        sam_processed_df = run_sam_processing(self.spark, alignment_df)
        
        # SAM 처리 결과 저장
        sam_processed_file = split_results_dir / "split_sam_processed_results.parquet"
        sam_processed_df.write.mode("overwrite").parquet(str(sam_processed_file))
        
        step3_time = time.time() - step3_start
        
        # DataFrame 액션을 한 번에 실행하여 중복 실행 방지
        sam_processed_results = sam_processed_df.collect()
        success_count = sum(1 for row in sam_processed_results if row["samtools_result"]["status"] == "success")
        total_count = len(sam_processed_results)
        
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
        
        # 4단계: BAM 파일 합치기 (핵심!)
        logger.info("\n" + "=" * 50)
        logger.info("4단계: BAM 파일 합치기 (분할 처리의 핵심)")
        logger.info("=" * 50)
        
        step4_start = time.time()
        
        # 성공한 BAM 파일들 수집
        bam_files = []
        for row in sam_processed_results:
            if row["samtools_result"]["status"] == "success":
                bam_files.append(row["samtools_result"]["bam_file"])
        
        if not bam_files:
            logger.error("합칠 BAM 파일이 없습니다.")
            return self.results
        
        # BAM 파일 합치기
        merged_bam_file = split_results_dir / "merged_split_output.bam"
        merge_success = self.merge_bam_files(bam_files, str(merged_bam_file))
        
        step4_time = time.time() - step4_start
        
        self.results["bam_merge"] = {
            "status": "completed" if merge_success else "failed",
            "input_files": len(bam_files),
            "output_file": str(merged_bam_file),
            "execution_time": step4_time
        }
        
        logger.info(f"BAM 파일 합치기 완료: {len(bam_files)}개 파일 -> {merged_bam_file} ({step4_time:.2f}초)")
        
        if not merge_success:
            logger.error("BAM 파일 합치기에 실패했습니다.")
            return self.results
        
        # 5단계: 커버리지 계산 (합쳐진 BAM 파일로 한 번만 계산)
        logger.info("\n" + "=" * 50)
        logger.info("5단계: 커버리지 계산 (합쳐진 BAM 파일로 한 번만)")
        logger.info("=" * 50)
        
        step5_start = time.time()
        
        # 합쳐진 BAM 파일로 커버리지 계산
        # SAM 처리 결과와 동일한 구조의 DataFrame 생성
        merged_sam_data = [{
            "sample_id": "merged_split_sample",
            "fastp_result": {"sample_id": "merged_split_sample", "status": "success"},
            "bwa_result": {"sample_id": "merged_split_sample", "status": "success"},
            "samtools_result": {
                "sample_id": "merged_split_sample",
                "status": "success",
                "bam_file": str(merged_bam_file)
            }
        }]
        
        merged_sam_df = self.spark.createDataFrame(merged_sam_data)
        
        # 커버리지 계산 실행 (split_pipeline 전용 결과 디렉토리 사용)
        # 임시로 PyToolsConfig.RESULTS_DIR을 split_results_dir로 변경
        original_results_dir = PyToolsConfig.RESULTS_DIR
        PyToolsConfig.RESULTS_DIR = split_results_dir
        
        try:
            coverage_df = run_coverage_calculation(self.spark, merged_sam_df, reference_index)
        finally:
            # 원래 결과 디렉토리로 복원
            PyToolsConfig.RESULTS_DIR = original_results_dir
            
        # 커버리지 결과 파일들을 올바른 위치로 이동
        coverage_results = coverage_df.collect()
        if coverage_results and coverage_results[0]["coverage_result"]["status"] == "success":
            coverage_result = coverage_results[0]["coverage_result"]
            
            # BED 파일 이동
            if coverage_result.get("bed_file"):
                bed_source = Path(coverage_result["bed_file"])
                bed_dest = split_results_dir / f"merged_split_sample_coverage.bed"
                if bed_source.exists():
                    shutil.move(str(bed_source), str(bed_dest))
                    logger.info(f"BED 파일 이동: {bed_source} -> {bed_dest}")
            
            # BigWig 파일 이동
            if coverage_result.get("bigwig_file"):
                bw_source = Path(coverage_result["bigwig_file"])
                bw_dest = split_results_dir / f"merged_split_sample.bw"
                if bw_source.exists():
                    shutil.move(str(bw_source), str(bw_dest))
                    logger.info(f"BigWig 파일 이동: {bw_source} -> {bw_dest}")
            
            # 통계 파일 이동
            if coverage_result.get("stats_file"):
                stats_source = Path(coverage_result["stats_file"])
                stats_dest = split_results_dir / f"merged_split_sample_pybedtools_coverage_stats.json"
                if stats_source.exists():
                    shutil.move(str(stats_source), str(stats_dest))
                    logger.info(f"통계 파일 이동: {stats_source} -> {stats_dest}")
        
        # 커버리지 결과 저장
        coverage_file = split_results_dir / "split_coverage_results.parquet"
        coverage_df.write.mode("overwrite").parquet(str(coverage_file))
        
        step5_time = time.time() - step5_start
        
        # 커버리지 결과 수집
        coverage_results = coverage_df.collect()
        if coverage_results:
            coverage_result = coverage_results[0]["coverage_result"]
        else:
            coverage_result = {"status": "failed", "error": "커버리지 계산 실패"}
        
        self.results["coverage"] = {
            "status": "completed",
            "coverage_result": coverage_result,
            "execution_time": step5_time
        }
        
        logger.info(f"커버리지 계산 완료: {coverage_result['status']} ({step5_time:.2f}초)")
        
        # 파이프라인 완료
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        self.results["pipeline"] = {
            "status": "completed",
            "total_execution_time": total_time,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "chunks_processed": len(chunk_pairs)
        }
        
        logger.info(f"\n분할된 데이터 파이프라인 완료!")
        logger.info(f"처리된 청크 수: {len(chunk_pairs)}개")
        logger.info(f"총 실행 시간: {total_time:.2f}초")
        logger.info(f"결과 저장 위치: {split_results_dir}")
        
        return self.results

def main():
    """메인 함수"""
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("SplitDataPipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "6") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .getOrCreate()
    
    try:
        # 파이프라인 실행
        pipeline = SplitDataPipeline(spark)
        
        # 경로 설정
        split_dir = Path("data/reads_chunks_correct")
        reference_genome = Config.REFERENCE_GENOME
        reference_index = Config.REFERENCE_INDEX
        
        # 파이프라인 실행
        results = pipeline.run_pipeline(split_dir, reference_genome, reference_index)
        
        # 결과 출력
        print("\n" + "=" * 60)
        print("분할된 데이터 파이프라인 결과")
        print("=" * 60)
        print(json.dumps(results, indent=2, default=str))
        
    except Exception as e:
        logger.error(f"파이프라인 실행 중 오류: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
