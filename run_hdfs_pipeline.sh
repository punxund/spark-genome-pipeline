#!/bin/bash

# HDFS 기반 Spark 파이프라인 실행 스크립트
# 사용법: ./run_hdfs_pipeline.sh [로컬_데이터_경로]

set -euo pipefail

LOCAL_DATA_DIR=${1:-"./data"}
HDFS_BASE="hdfs://hongsik1.vm.informatik.hu-berlin.de:9000"
HDFS_DATA_DIR="$HDFS_BASE/genome"

echo "=== HDFS 기반 Spark 파이프라인 실행 ==="
echo "로컬 데이터 경로: $LOCAL_DATA_DIR"
echo "HDFS 데이터 경로: $HDFS_DATA_DIR"

# 1. HDFS 디렉토리 생성
echo "=== 1단계: HDFS 디렉토리 생성 ==="
hdfs dfs -mkdir -p $HDFS_DATA_DIR/reads
hdfs dfs -mkdir -p $HDFS_DATA_DIR/results/hybrid_pipeline
hdfs dfs -mkdir -p $HDFS_DATA_DIR/temp

# 2. 로컬 데이터를 HDFS에 업로드
echo "=== 2단계: 데이터 업로드 ==="
if [ -d "$LOCAL_DATA_DIR/reads" ]; then
    echo "FASTQ 파일들을 HDFS에 업로드 중..."
    find $LOCAL_DATA_DIR/reads -name "*.fastq" -o -name "*.fastq.gz" | while read file; do
        echo "업로드 중: $file"
        hdfs dfs -put "$file" $HDFS_DATA_DIR/reads/
    done
else
    echo "경고: $LOCAL_DATA_DIR/reads 디렉토리를 찾을 수 없습니다."
fi

# 참조 게놈 업로드
if [ -f "$LOCAL_DATA_DIR/ref_sequence_genB.fa" ]; then
    echo "참조 게놈을 HDFS에 업로드 중..."
    hdfs dfs -put "$LOCAL_DATA_DIR/ref_sequence_genB.fa" $HDFS_DATA_DIR/
    hdfs dfs -put "$LOCAL_DATA_DIR/ref_sequence_genB.fa.fai" $HDFS_DATA_DIR/ 2>/dev/null || true
else
    echo "경고: 참조 게놈 파일을 찾을 수 없습니다."
fi

# 3. 업로드된 데이터 확인
echo "=== 3단계: 업로드된 데이터 확인 ==="
echo "HDFS 디렉토리 구조:"
hdfs dfs -ls -R $HDFS_DATA_DIR

echo "데이터 분산 상태:"
hdfs dfs -du -h $HDFS_DATA_DIR

# 4. Spark 파이프라인 실행
echo "=== 4단계: Spark 파이프라인 실행 ==="
cd /workspace/pipeline

# Spark 설정으로 파이프라인 실행
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
echo "HDFS 결과 디렉토리:"
hdfs dfs -ls -R $HDFS_DATA_DIR/results/

echo "로컬 결과 파일:"
ls -la ./data/temp/
