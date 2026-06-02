#!/usr/bin/env bash
# main.py 를 Spark로 실행: FASTQ/참조는 HDFS data 영역(예: /genome/reads)에서 읽고,
# 결과( parquet, BAM, 로그 )는 지역 results/original_pipeline/ 에 씁니다.
# Prerequisite: driver·executor(로컬 모드는 동일 프로세스)에 `hdfs` CLI와 fastp, bwa, samtools, bedtools, bedGraphToBigWig.
#
# 사용 예:
#   export HADOOP_CONF_DIR=/path/to/hadoop-config
#   export PATH="$PATH:/path/to/hadoop-3.3.6/bin"
#   export MAIN_PIPELINE_HDFS_READS=hdfs://127.0.0.1:9000/genome/reads
#   export MAIN_PIPELINE_HDFS_REF=hdfs://127.0.0.1:9000/genome/ref_sequence_genB.fa
#   export MAIN_PIPELINE_HDFS_FAI=hdfs://127.0.0.1:9000/genome/ref_sequence_genB.fa.fai
#   ./run_main_spark_hdfs.sh
#
# 또는 인자로:
#   ./run_main_spark_hdfs.sh "hdfs://nn:9000/genome/reads" "hdfs://nn:9000/genome/ref_sequence_genB.fa" "hdfs://nn:9000/genome/ref_sequence_genB.fa.fai" "local[*]"

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

HDFS_READS="${1:-${MAIN_PIPELINE_HDFS_READS:-}}"
HDFS_REF="${2:-${MAIN_PIPELINE_HDFS_REF:-}}"
HDFS_FAI="${3:-${MAIN_PIPELINE_HDFS_FAI:-}}"
SPARK_M="${4:-${SPARK_MAIN_SPARK_MASTER:-${MAIN_SPARK_MASTER:-local[*]}}}"

if [[ -z "$HDFS_READS" || -z "$HDFS_REF" || -z "$HDFS_FAI" ]]; then
  echo "Usage: $0 <hdfs-reads-dir> <hdfs-ref.fa> <hdfs-ref.fa.fai> [spark-master]" >&2
  echo "Or set MAIN_PIPELINE_HDFS_READS, MAIN_PIPELINE_HDFS_REF, MAIN_PIPELINE_HDFS_FAI" >&2
  exit 1
fi

export MAIN_PIPELINE_HDFS_READS="$HDFS_READS"
export MAIN_PIPELINE_HDFS_REF="$HDFS_REF"
export MAIN_PIPELINE_HDFS_FAI="$HDFS_FAI"
export SPARK_MAIN_SPARK_MASTER="$SPARK_M"

PY_FILES=$(printf '%s' "config.py,utils.py,hdfs_path_utils.py,preprocessing.py,alignment.py,sam_processing.py,coverage.py")
if command -v spark-submit >/dev/null 2>&1; then
  spark-submit --master "$SPARK_M" \
    --py-files "$PY_FILES" \
    main.py \
    --spark-master "$SPARK_M" \
    --reference-genome "$HDFS_REF" \
    --reference-index "$HDFS_FAI"
else
  # spark-submit 이 없으면 동일 머신에서 python 직접 (로컬 Spark)
  SPARK_MAIN_SPARK_MASTER="$SPARK_M" \
    MAIN_PIPELINE_HDFS_READS="$HDFS_READS" \
    MAIN_PIPELINE_HDFS_REF="$HDFS_REF" \
    MAIN_PIPELINE_HDFS_FAI="$HDFS_FAI" \
  python3 main.py \
    --spark-master "$SPARK_M" \
    --reference-genome "$HDFS_REF" \
    --reference-index "$HDFS_FAI"
fi
