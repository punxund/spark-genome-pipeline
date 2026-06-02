from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import logging
from pathlib import Path
from typing import List, Tuple, Optional, Union
import tempfile
import shutil

from config import Config
from utils import run_command, check_tool_availability, create_temp_file, cleanup_temp_files
from hdfs_path_utils import is_hdfs_uri, local_scratch, download_if_hdfs

logger = logging.getLogger(__name__)

def run_fastp_udf(sample_id: str, r1_file: str, r2_file: str) -> dict:
    """
    fastp를 실행하는 UDF 함수
    
    Args:
        sample_id: 샘플 ID
        r1_file: R1 파일 경로
        r2_file: R2 파일 경로
    
    Returns:
        처리 결과 딕셔너리
    """
    temp_files = []
    work = local_scratch("orig_fp_")
    try:
        r1_local = download_if_hdfs(r1_file, work) if is_hdfs_uri(str(r1_file)) else r1_file
        r2_local = download_if_hdfs(r2_file, work) if is_hdfs_uri(str(r2_file)) else r2_file

        # 임시 출력 파일 생성
        output_r1 = create_temp_file(suffix=".trimmed.fastq", prefix=f"{sample_id}_R1_")
        output_r2 = create_temp_file(suffix=".trimmed.fastq", prefix=f"{sample_id}_R2_")
        report_json = create_temp_file(suffix=".json", prefix=f"{sample_id}_fastp_")
        report_html = create_temp_file(suffix=".html", prefix=f"{sample_id}_fastp_")
        
        temp_files.extend([output_r1, output_r2, report_json, report_html])
        
        # fastp 명령어 실행
        cmd = [
            Config.FASTP_PATH,
            "-i", r1_local,
            "-I", r2_local,
            "-o", str(output_r1),
            "-O", str(output_r2),
            "--detect_adapter_for_pe",
            "--json", str(report_json),
            "--html", str(report_html),
            "--thread", "16"  # 16개 스레드로 실행 (genCov와 동일)
        ]
        
        result = run_command(cmd, check=False)
        
        if result.returncode == 0:
            # 결과 파일들을 결과 디렉토리로 복사
            final_r1 = Config.RESULTS_DIR / f"{sample_id}.R1.trimmed.fastq"
            final_r2 = Config.RESULTS_DIR / f"{sample_id}.R2.trimmed.fastq"
            final_json = Config.RESULTS_DIR / f"report_{sample_id}_fastp.json"
            final_html = Config.RESULTS_DIR / f"report_{sample_id}_fastp.html"
            
            shutil.copy2(output_r1, final_r1)
            shutil.copy2(output_r2, final_r2)
            shutil.copy2(report_json, final_json)
            shutil.copy2(report_html, final_html)
            
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
        cleanup_temp_files(temp_files)
        shutil.rmtree(work, ignore_errors=True)

class FastQPreprocessor:
    """Spark를 사용한 FASTQ 전처리 클래스"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.temp_files = []
        
        # fastp 도구 사용 가능 여부 확인
        if not check_tool_availability("fastp", Config.FASTP_PATH):
            raise RuntimeError("fastp 도구를 찾을 수 없습니다. 설치 후 다시 시도하세요.")
    
    def create_fastq_dataframe(self, fastq_pairs: List[Tuple[str, str, str]]) -> "pyspark.sql.DataFrame":
        """
        FASTQ 파일 쌍으로부터 Spark DataFrame을 생성합니다.
        
        Args:
            fastq_pairs: (샘플ID, R1 경로, R2 경로) — str (로컬 또는 hdfs://)
        
        Returns:
            Spark DataFrame
        """
        from hdfs_path_utils import hdfs_file_size

        def _size_mb(p: str) -> str:
            if is_hdfs_uri(str(p)):
                sz = hdfs_file_size(str(p))
                if sz is None:
                    return "0.00"
                return f"{sz / (1024 * 1024):.2f}"
            return f"{Path(p).stat().st_size / (1024*1024):.2f}"

        # DataFrame 스키마 정의
        schema = StructType([
            StructField("sample_id", StringType(), False),
            StructField("r1_file", StringType(), False),
            StructField("r2_file", StringType(), False),
            StructField("r1_size_mb", StringType(), True),
            StructField("r2_size_mb", StringType(), True)
        ])
        
        # 데이터 준비
        data = []
        for sample_id, r1_file, r2_file in fastq_pairs:
            r1_size = _size_mb(r1_file)
            r2_size = _size_mb(r2_file)
            
            data.append({
                "sample_id": sample_id,
                "r1_file": str(r1_file),
                "r2_file": str(r2_file),
                "r1_size_mb": r1_size,
                "r2_size_mb": r2_size
            })
        
        logger.info(f"FASTQ DataFrame 생성: {len(data)}개 샘플")
        return self.spark.createDataFrame(data, schema)
    
    def process_fastq_files(self, fastq_pairs: List[Tuple[str, str, str]]) -> "pyspark.sql.DataFrame":
        """
        FASTQ 파일들을 전처리합니다.
        
        Args:
            fastq_pairs: (샘플ID, R1 경로, R2 경로) 튜플 리스트 (str)
        
        Returns:
            처리 결과 DataFrame
        """
        logger.info("FASTQ 전처리 시작")
        
        # DataFrame 생성
        df = self.create_fastq_dataframe(fastq_pairs)
        
        # UDF 등록
        fastp_udf = udf(run_fastp_udf, returnType=StructType([
            StructField("sample_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("trimmed_r1", StringType(), True),
            StructField("trimmed_r2", StringType(), True),
            StructField("report_json", StringType(), True),
            StructField("report_html", StringType(), True),
            StructField("error", StringType(), True)
        ]))
        
        # fastp 실행
        result_df = df.withColumn(
            "fastp_result", 
            fastp_udf(col("sample_id"), col("r1_file"), col("r2_file"))
        )
        
        # 결과 확인
        success_count = result_df.filter(col("fastp_result.status") == "success").count()
        total_count = result_df.count()
        
        logger.info(f"전처리 완료: {success_count}/{total_count} 성공")
        
        return result_df
    
    def cleanup(self):
        """임시 파일들을 정리합니다."""
        cleanup_temp_files(self.temp_files)
        self.temp_files.clear()

def run_preprocessing(
    spark: SparkSession, reads_dir: Optional[Union[Path, str]] = None
) -> "pyspark.sql.DataFrame":
    """
    전처리 파이프라인을 실행합니다.
    
    Args:
        spark: SparkSession
        reads_dir: 로컬 읽기 디렉터리 또는 HDFS (hdfs://.../.../reads)
    
    Returns:
        처리 결과 DataFrame
    """
    from utils import get_fastq_pairs_for_pipeline

    fastq_pairs = get_fastq_pairs_for_pipeline(reads_dir)
    
    if not fastq_pairs:
        rdir = reads_dir if reads_dir is not None else Config.READS_DIR
        raise ValueError(f"FASTQ 파일 쌍을 찾을 수 없습니다: {rdir}")
    
    # 전처리 실행
    preprocessor = FastQPreprocessor(spark)
    try:
        result_df = preprocessor.process_fastq_files(fastq_pairs)
        return result_df
    finally:
        preprocessor.cleanup()
