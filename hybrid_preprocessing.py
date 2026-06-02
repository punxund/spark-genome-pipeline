"""
Hybrid 전처리: 입·출력을 HDFS에 맞춤 (fastp는 로컬 temp → HDFS 업로드).
"""
import logging
import shutil
from pathlib import Path
from typing import List, Tuple, Union

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType

from config_hybrid import HybridConfig as Config
from hdfs_path_utils import (
    download_if_hdfs,
    hdfs_file_size,
    is_hdfs_uri,
    local_scratch,
    parse_fastq_pairs_hdfs,
    put_file_to_dir,
)
from utils import check_tool_availability, create_temp_file, cleanup_temp_files, run_command

logger = logging.getLogger(__name__)


def run_fastp_udf_hybrid(sample_id: str, r1_file: str, r2_file: str) -> dict:
    """fastp 후 trimmed/리포트를 HDFS(HDFS_WORK_TRIM)에 올리고 hdfs 경로를 반환."""
    temp_files = []
    work = local_scratch()
    try:
        r1_local = download_if_hdfs(r1_file, work)
        r2_local = download_if_hdfs(r2_file, work)

        output_r1 = create_temp_file(suffix=".trimmed.fastq", prefix=f"{sample_id}_R1_")
        output_r2 = create_temp_file(suffix=".trimmed.fastq", prefix=f"{sample_id}_R2_")
        report_json = create_temp_file(suffix=".json", prefix=f"{sample_id}_fastp_")
        report_html = create_temp_file(suffix=".html", prefix=f"{sample_id}_fastp_")
        temp_files.extend([output_r1, output_r2, report_json, report_html])

        cmd = [
            Config.FASTP_PATH,
            "-i", r1_local,
            "-I", r2_local,
            "-o", str(output_r1),
            "-O", str(output_r2),
            "--detect_adapter_for_pe",
            "--json", str(report_json),
            "--html", str(report_html),
            "--thread", "16",
        ]
        result = run_command(cmd, check=False)

        if result.returncode != 0:
            return {
                "sample_id": sample_id,
                "status": "failed",
                "trimmed_r1": None,
                "trimmed_r2": None,
                "report_json": None,
                "report_html": None,
                "error": result.stderr,
            }

        h1 = put_file_to_dir(Path(output_r1), Config.HDFS_WORK_TRIM)
        h2 = put_file_to_dir(Path(output_r2), Config.HDFS_WORK_TRIM)
        hj = put_file_to_dir(Path(report_json), Config.HDFS_WORK_TRIM)
        hh = put_file_to_dir(Path(report_html), Config.HDFS_WORK_TRIM)
        return {
            "sample_id": sample_id,
            "status": "success",
            "trimmed_r1": h1,
            "trimmed_r2": h2,
            "report_json": hj,
            "report_html": hh,
            "error": None,
        }
    except Exception as e:
        logger.error("fastp hybrid UDF: %s", e)
        return {
            "sample_id": sample_id,
            "status": "error",
            "trimmed_r1": None,
            "trimmed_r2": None,
            "report_json": None,
            "report_html": None,
            "error": str(e),
        }
    finally:
        cleanup_temp_files(temp_files)
        shutil.rmtree(work, ignore_errors=True)


class FastQPreprocessorHybrid:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        if not check_tool_availability("fastp", Config.FASTP_PATH):
            raise RuntimeError("fastp not found")

    def create_dataframe(
        self, fastq_triples: List[Tuple[str, str, str]]
    ) -> "pyspark.sql.DataFrame":
        from pyspark.sql.types import StringType
        from pyspark.sql.types import StructType, StructField

        schema = StructType(
            [
                StructField("sample_id", StringType(), False),
                StructField("r1_file", StringType(), False),
                StructField("r2_file", StringType(), False),
                StructField("r1_size_mb", StringType(), True),
                StructField("r2_size_mb", StringType(), True),
            ]
        )
        data = []
        for sample_id, r1, r2 in fastq_triples:
            def sz(p: str) -> str:
                if is_hdfs_uri(p):
                    b = hdfs_file_size(p)
                    return f"{(b or 0) / (1024 * 1024):.2f}" if b is not None else "0.00"
                try:
                    return f"{Path(p).stat().st_size / (1024*1024):.2f}"
                except OSError:
                    return "0.00"

            data.append(
                {
                    "sample_id": sample_id,
                    "r1_file": r1,
                    "r2_file": r2,
                    "r1_size_mb": sz(r1),
                    "r2_size_mb": sz(r2),
                }
            )
        return self.spark.createDataFrame(data, schema)

    def process(
        self, fastq_triples: List[Tuple[str, str, str]]
    ) -> "pyspark.sql.DataFrame":
        df = self.create_dataframe(fastq_triples)
        fastp_udf = udf(
            run_fastp_udf_hybrid,
            returnType=StructType(
                [
                    StructField("sample_id", StringType(), False),
                    StructField("status", StringType(), False),
                    StructField("trimmed_r1", StringType(), True),
                    StructField("trimmed_r2", StringType(), True),
                    StructField("report_json", StringType(), True),
                    StructField("report_html", StringType(), True),
                    StructField("error", StringType(), True),
                ]
            ),
        )
        return df.withColumn(
            "fastp_result",
            fastp_udf(col("sample_id"), col("r1_file"), col("r2_file")),
        )


def run_preprocessing(
    spark: SparkSession, reads_dir: Union[str, Path, None] = None
) -> "pyspark.sql.DataFrame":
    rdir = reads_dir if reads_dir is not None else Config.HDFS_READS_DIR
    sdir = str(rdir)
    if sdir.startswith("hdfs://"):
        triples = parse_fastq_pairs_hdfs(sdir)
    else:
        from utils import parse_fastq_pairs

        pairs = parse_fastq_pairs(Path(sdir))
        triples = [(a, str(b), str(c)) for a, b, c in pairs]
    if not triples:
        raise ValueError(f"No FASTQ pairs: {rdir}")
    p = FastQPreprocessorHybrid(spark)
    return p.process(triples)
