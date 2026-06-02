#!/bin/bash

# NFS 기반 Spark 파이프라인 실행 스크립트
# HDFS 대신 NFS를 사용하여 데이터 공유

set -euo pipefail

LOCAL_DATA_DIR=${1:-"./data"}
NFS_DATA_DIR="/mnt/genome"

echo "=== NFS 기반 Spark 파이프라인 실행 ==="
echo "로컬 데이터 경로: $LOCAL_DATA_DIR"
echo "NFS 데이터 경로: $NFS_DATA_DIR"

# 1. NFS 디렉토리 생성
echo "=== 1단계: NFS 디렉토리 생성 ==="
sudo mkdir -p $NFS_DATA_DIR/reads
sudo mkdir -p $NFS_DATA_DIR/results/hybrid_pipeline
sudo mkdir -p $NFS_DATA_DIR/temp

# 2. 로컬 데이터를 NFS에 복사
echo "=== 2단계: 데이터 복사 ==="
if [ -d "$LOCAL_DATA_DIR/reads" ]; then
    echo "FASTQ 파일들을 NFS에 복사 중..."
    sudo cp -r $LOCAL_DATA_DIR/reads/* $NFS_DATA_DIR/reads/
else
    echo "경고: $LOCAL_DATA_DIR/reads 디렉토리를 찾을 수 없습니다."
fi

# 참조 게놈 복사
if [ -f "$LOCAL_DATA_DIR/ref_sequence_genB.fa" ]; then
    echo "참조 게놈을 NFS에 복사 중..."
    sudo cp "$LOCAL_DATA_DIR/ref_sequence_genB.fa" $NFS_DATA_DIR/
    sudo cp "$LOCAL_DATA_DIR/ref_sequence_genB.fa.fai" $NFS_DATA_DIR/ 2>/dev/null || true
else
    echo "경고: 참조 게놈 파일을 찾을 수 없습니다."
fi

# 3. 복사된 데이터 확인
echo "=== 3단계: 복사된 데이터 확인 ==="
echo "NFS 디렉토리 구조:"
ls -la $NFS_DATA_DIR/

echo "데이터 크기:"
du -h $NFS_DATA_DIR/

# 4. Spark 파이프라인 실행
echo "=== 4단계: Spark 파이프라인 실행 ==="
cd /workspace/pipeline

# NFS 기반으로 파이프라인 실행
spark-submit \
    --master spark://141.20.38.81:7077 \
    --driver-memory 4G \
    --executor-memory 4G \
    --executor-cores 6 \
    --num-executors 3 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    main_hybrid.py

echo "=== 파이프라인 실행 완료 ==="

# 5. 결과 확인
echo "=== 5단계: 결과 확인 ==="
echo "NFS 결과 디렉토리:"
ls -la $NFS_DATA_DIR/results/

echo "로컬 결과 파일:"
ls -la ./data/temp/




