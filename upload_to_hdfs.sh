#!/bin/bash

# HDFS에 데이터 업로드 스크립트
# 사용법: ./upload_to_hdfs.sh [로컬_데이터_경로]
# 
# 데이터 플로우:
# 로컬 컴퓨터 → VM들 (hongsik1-4) → HDFS 분산 저장

set -euo pipefail

LOCAL_DATA_DIR=${1:-"./data/reads"}
HDFS_DATA_DIR="/genome/reads"
VM_HOST="kimhongs@hongsik1.vm.informatik.hu-berlin.de"

echo "=== HDFS 데이터 업로드 시작 ==="
echo "로컬 경로: $LOCAL_DATA_DIR"
echo "VM 호스트: $VM_HOST"
echo "HDFS 경로: $HDFS_DATA_DIR"
echo ""
echo "데이터가 VM들(hongsik1-4)의 디스크에 분산 저장됩니다:"
echo "  - hongsik1: /opt/hadoop/data/datanode/"
echo "  - hongsik2: /opt/hadoop/data/datanode/"
echo "  - hongsik3: /opt/hadoop/data/datanode/"
echo "  - hongsik4: /opt/hadoop/data/datanode/"
echo ""
echo "HDFS 블록 설정:"
echo "  - 블록 크기: 128MB (134,217,728 bytes)"
echo "  - 큰 파일은 128MB 단위로 자동 분할되어 여러 VM에 분산 저장"
echo "  - 복제본: 1개 (4개 VM에 분산)"

# HDFS 디렉토리 생성
echo "HDFS 디렉토리 생성 중..."
hdfs dfs -mkdir -p $HDFS_DATA_DIR

# FASTQ 파일들을 HDFS에 업로드 (병렬 처리)
echo "FASTQ 파일 업로드 중..."
find $LOCAL_DATA_DIR -name "*.fastq" -o -name "*.fastq.gz" | while read file; do
    echo "업로드 중: $file"
    hdfs dfs -put "$file" $HDFS_DATA_DIR/
done

# 업로드된 파일 목록 확인
echo "=== 업로드 완료 ==="
hdfs dfs -ls $HDFS_DATA_DIR

# 데이터 분산 상태 확인
echo "=== 데이터 분산 상태 ==="
hdfs dfs -du -h $HDFS_DATA_DIR
