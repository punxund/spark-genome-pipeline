#!/usr/bin/env bash
set -euo pipefail

# Start HDFS NameNode in Docker on hongsik1

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== Starting HDFS NameNode container (hdfs-namenode) on $(hostname) ==="

# Clean up any existing container
if docker ps -a --format '{{.Names}}' | grep -q '^hdfs-namenode$'; then
  echo "[INFO] Stopping existing hdfs-nodenode container..."
  docker stop hdfs-namenode || true
  docker rm   hdfs-namenode || true
fi

mkdir -p hadoop-data hadoop-logs

echo "[INFO] Launching new hdfs-namenode container..."
docker run -d --name hdfs-namenode \
  -p 9000:9000 -p 9870:9870 \
  -v "$PWD/hadoop-data:/opt/hadoop/data" \
  -v "$PWD/hadoop-logs:/opt/hadoop/logs" \
  -v "$PWD/hadoop-config/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" \
  -v "$PWD/hadoop-config/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro" \
  -e JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
  -e HDFS_NAMENODE_USER=root \
  -e HDFS_DATANODE_USER=root \
  -e HDFS_SECONDARYNAMENODE_USER=root \
  ec6a7cf0a97a \
  bash -lc 'mkdir -p /opt/hadoop/data/namenode /opt/hadoop/data/datanode; \
            if [ ! -d /opt/hadoop/data/namenode/current ]; then \
              echo "[INFO] Formatting NameNode..."; \
              /opt/hadoop/bin/hdfs namenode -format -force; \
            else \
              echo "[INFO] NameNode storage already formatted, skipping format."; \
            fi; \
            echo "[INFO] Starting NameNode..."; \
            exec /opt/hadoop/bin/hdfs namenode'


