#!/usr/bin/env bash
# VM에서 main_hybrid.py 를 Spark로 실행 (Hadoop 설정·필수 도구는 VM에 있어야 함)
set -euo pipefail
REPO="$(cd "$(dirname "$0")" && pwd)"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$REPO/hadoop-config}"
export HADOOP_HOME="${HADOOP_HOME:-}"
# PySpark/executor가 HDFS defaultFS 를 읽도록
if [[ -z "${PYTHONPATH:-}" ]]; then
  export PYTHONPATH="$REPO"
else
  export PYTHONPATH="$REPO:$PYTHONPATH"
fi

# Hadoop 클라이언트( hdfs dfs ) — driver·executor UDF(hdfs_path_utils)에 필요
if ! command -v hdfs &>/dev/null; then
  echo "[WARN] 'hdfs' CLI 가 PATH에 없습니다. hybrid UDF(get/put)은 Hadoop 클라이언트가 필요합니다." >&2
  echo "       VM에 Apache Hadoop client 설치 후 PATH에 추가하세요 (예: export PATH=\$HADOOP_HOME/bin:\$PATH)" >&2
fi

# Spark: 기본은 config_hybrid.py 의 SPARK_MASTER. 오버라이드: SPARK_MASTER 환경변수
SPM="${SPARK_MASTER:-spark://141.20.38.81:7077}"
READS="${READS_DIR:-}"   # 비우면 HDFS 읽기 (config)
REFG="${REFERENCE_GENOME:-}"
REFI="${REFERENCE_INDEX:-}"

# executor UDF import용(공유 FS가 아닌/worker에 코드 없을 때). 공유 볼륨이면 생략 가능.
cd "$REPO"
PY_LIST="config_hybrid.py,utils.py,hdfs_path_utils.py,hybrid_preprocessing.py,hybrid_alignment.py"
PY_LIST+=",pythontools/hybrid_sam_processing.py,pythontools/hybrid_coverage.py"
if [[ -f pythontools/__init__.py ]]; then
  PY_LIST="pythontools/__init__.py,$PY_LIST"
fi

# 로컬로만 먼저 볼 때: export SPARK_MASTER=local[2]
echo "[INFO] HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "[INFO] SPARK_MASTER=$SPM"
echo "[INFO] (cwd=$PWD)"
exec spark-submit \
  --master "$SPM" \
  --py-files "$PY_LIST" \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-6g}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-6g}" \
  main_hybrid.py \
  --spark-master "$SPM" \
  ${READS:+--reads-dir "$READS"} \
  ${REFG:+--reference-genome "$REFG"} \
  ${REFI:+--reference-index "$REFI"}
