#!/usr/bin/env bash
set -euo pipefail

# Start an HDFS DataNode in Docker on hongsik2/3/4
#
# Usage:
#   ./scripts/start_hdfs_datanode.sh <id> <host_port>
#   e.g. on hongsik2: ./scripts/start_hdfs_datanode.sh 2 9866

ID="${1:-2}"
PORT="${2:-9866}"
NAME="hdfs-datanode-${ID}"

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== Starting HDFS DataNode container ($NAME) on $(hostname) ==="

if docker ps -a --format '{{.Names}}' | grep -q "^${NAME}\$"; then
  echo "[INFO] Stopping existing ${NAME}..."
  docker stop "${NAME}" || true
  docker rm   "${NAME}" || true
fi

mkdir -p hadoop-data hadoop-logs

echo "[INFO] Launching new ${NAME} on host port ${PORT} -> container 9866 ..."
docker run -d --name "${NAME}" \
  -p "${PORT}:9866" \
  -v "$PWD/hadoop-data:/opt/hadoop/data" \
  -v "$PWD/hadoop-logs:/opt/hadoop/logs" \
  -v "$PWD/hadoop-config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" \
  -v "$PWD/hadoop-config/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro" \
  -e JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
  -e HDFS_NAMENODE_USER=root \
  -e HDFS_DATANODE_USER=root \
  -e HDFS_SECONDARYNAMENODE_USER=root \
  ec6a7cf0a97a \
  bash -lc 'mkdir -p /opt/hadoop/data/datanode; \
            echo "[INFO] Starting DataNode..."; \
            exec /opt/hadoop/bin/hdfs datanode'


