#!/usr/bin/env bash
# hongsik1: docker namenode에서 HDFS에 put. 컨테이너 내부는 -fs hdfs://127.0.0.1:9000 필수.
set -euo pipefail

REPO="${REPO:-/vol/fob-vol7/mi21/kimhongs/spark-genome-pipeline}"
NN_FS="hdfs://127.0.0.1:9000"
CONTAINER_STAGING="/tmp/hdfs-staging-$$"

hdfs_docker() {
  docker exec hdfs-namenode /opt/hadoop/bin/hdfs dfs -fs "$NN_FS" "$@"
}

if [[ ! -d "$REPO/data/reads" ]]; then
  echo "ERR: $REPO/data/reads 가 없습니다." >&2
  exit 1
fi

echo "=== HDFS 디렉터리 ==="
hdfs_docker -mkdir -p /genome/reads

echo "=== ref_sequence_genB.fa* → /genome/ ==="
docker exec hdfs-namenode mkdir -p "$CONTAINER_STAGING"
for f in "$REPO"/data/ref_sequence_genB.fa*; do
  [[ -e "$f" ]] || continue
  b=$(basename "$f")
  echo "  $b"
  docker cp "$f" "hdfs-namenode:${CONTAINER_STAGING}/$b"
  hdfs_docker -put -f "${CONTAINER_STAGING}/$b" /genome/
  docker exec hdfs-namenode rm -f "${CONTAINER_STAGING}/$b" || true
done

echo "=== reads/*.fastq* → /genome/reads/ ==="
for f in "$REPO"/data/reads/*.fastq*; do
  [[ -e "$f" ]] || continue
  b=$(basename "$f")
  echo "  $b"
  docker cp "$f" "hdfs-namenode:${CONTAINER_STAGING}/$b"
  hdfs_docker -put -f "${CONTAINER_STAGING}/$b" /genome/reads/
  docker exec hdfs-namenode rm -f "${CONTAINER_STAGING}/$b" || true
done

docker exec hdfs-namenode rmdir "$CONTAINER_STAGING" 2>/dev/null || true

echo "=== HDFS ls ==="
hdfs_docker -ls /genome/
hdfs_docker -ls /genome/reads/ | head -30
hdfs_docker -du -h /genome/
echo OK
