"""Hybrid BWA: SAM 산출을 HDFS(HDFS_WORK_SAM)에 저장."""
import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StructField, StringType, StructType

from config_hybrid import HybridConfig as Config
from hdfs_path_utils import (
    download_if_hdfs,
    download_reference_fasta_and_index,
    local_scratch,
    put_file_to_dir,
)
from utils import check_tool_availability, create_temp_file, cleanup_temp_files, run_command

if TYPE_CHECKING:
    import pyspark.sql

logger = logging.getLogger(__name__)


def run_bwa_mem_udf_hybrid(
    sample_id: str, r1_file: str, r2_file: str, reference_genome: str
) -> dict:
    work = local_scratch()
    temp_files = []
    try:
        r1 = download_if_hdfs(r1_file, work)
        r2 = download_if_hdfs(r2_file, work)
        ref_local = download_reference_fasta_and_index(reference_genome, work)

        index_files = [
            f"{ref_local}.amb",
            f"{ref_local}.ann",
            f"{ref_local}.bwt",
            f"{ref_local}.pac",
            f"{ref_local}.sa",
        ]
        if not all(Path(f).exists() for f in index_files):
            idx_cmd = [Config.BWA_PATH, "index", ref_local]
            ir = run_command(idx_cmd, check=False)
            if ir.returncode != 0:
                return {
                    "sample_id": sample_id,
                    "status": "failed",
                    "sam_file": None,
                    "error": f"bwa index: {ir.stderr}",
                }

        sam_tmp = create_temp_file(suffix=".sam", prefix=f"{sample_id}_")
        temp_files.append(sam_tmp)

        cmd = [Config.BWA_PATH, "mem", "-t", "16", ref_local, r1, r2]
        with open(sam_tmp, "w") as f:
            result = run_command(cmd, check=False)
            if result.returncode == 0:
                f.write(result.stdout)
            else:
                return {
                    "sample_id": sample_id,
                    "status": "failed",
                    "sam_file": None,
                    "error": result.stderr,
                }

        dest = put_file_to_dir(Path(sam_tmp), Config.HDFS_WORK_SAM)
        return {
            "sample_id": sample_id,
            "status": "success",
            "sam_file": dest,
            "error": None,
        }
    except Exception as e:
        logger.exception("bwa hybrid: %s", sample_id)
        return {
            "sample_id": sample_id,
            "status": "error",
            "sam_file": None,
            "error": str(e),
        }
    finally:
        cleanup_temp_files(temp_files)
        shutil.rmtree(work, ignore_errors=True)


class BWAAlignerHybrid:
    def __init__(self, spark: SparkSession, reference_genome: Optional[str] = None):
        self.spark = spark
        self.reference_genome = reference_genome or Config.HDFS_REFERENCE_GENOME
        if not check_tool_availability("bwa", Config.BWA_PATH):
            raise RuntimeError("bwa not found")

    def process_alignment(
        self, preprocessed_df: "pyspark.sql.DataFrame"
    ) -> "pyspark.sql.DataFrame":
        from pyspark.sql.functions import col as C

        ok = preprocessed_df.filter(C("fastp_result.status") == "success")
        if ok.count() == 0:
            return self.spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField("sample_id", StringType(), False),
                        StructField("status", StringType(), False),
                        StructField("sam_file", StringType(), True),
                        StructField("error", StringType(), True),
                    ]
                ),
            )

        bwa_udf = udf(
            run_bwa_mem_udf_hybrid,
            returnType=StructType(
                [
                    StructField("sample_id", StringType(), False),
                    StructField("status", StringType(), False),
                    StructField("sam_file", StringType(), True),
                    StructField("error", StringType(), True),
                ]
            ),
        )
        return ok.withColumn(
            "bwa_result",
            bwa_udf(
                col("fastp_result.sample_id"),
                col("fastp_result.trimmed_r1"),
                col("fastp_result.trimmed_r2"),
                lit(str(self.reference_genome)),
            ),
        )


def run_alignment(
    spark: SparkSession,
    preprocessed_df: "pyspark.sql.DataFrame",
    reference_genome: Union[str, Path, None] = None,
) -> "pyspark.sql.DataFrame":
    ref = reference_genome or Config.HDFS_REFERENCE_GENOME
    if isinstance(ref, Path):
        ref = str(ref)
    a = BWAAlignerHybrid(spark, ref)
    return a.process_alignment(preprocessed_df)
