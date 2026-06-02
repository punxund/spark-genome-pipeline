#!/usr/bin/env bash
# gruenau1: HDFS(클러스터) + 로컬 Spark + 프로젝트 .tools/hadoop의 hdfs
set -euo pipefail
REPO="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$REPO/hadoop-config}"
export HADOOP_HOME="${HADOOP_HOME:-$REPO/.tools/hadoop-3.3.6}"
export PATH="$HADOOP_HOME/bin:$PATH"
export PYTHONPATH="$REPO"
# HDFS: 기본 /genome (config_hybrid) — HDFS_DATA_NAMESPACE 로 변경 가능
# Spark: local[N] = 드라이버·익스큐터 동일 머신(executor에서도 hdfs 사용 가능)
export SPARK_MASTER="${SPARK_MASTER:-local[4]}"
LOG="${1:-$REPO/data/temp/hybrid_full_pipeline.log}"
mkdir -p "$(dirname "$LOG")"
PY="config_hybrid.py,utils.py,hdfs_path_utils.py,hybrid_preprocessing.py,hybrid_alignment.py"
PY+=",pythontools/hybrid_sam_processing.py,pythontools/hybrid_coverage.py"
if [[ -f pythontools/__init__.py ]]; then
  PY="pythontools/__init__.py,$PY"
fi
echo "Logging to $LOG"
echo "SPARK_MASTER=$SPARK_MASTER HADOOP_CONF_DIR=$HADOOP_CONF_DIR" | tee -a "$LOG"
exec spark-submit \
  --master "$SPARK_MASTER" \
  --py-files "$PY" \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-6g}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY:-6g}" \
  main_hybrid.py --spark-master "$SPARK_MASTER" 2>&1 | tee -a "$LOG"
