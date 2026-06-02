#!/bin/bash

# HDFS에 데이터 업로드 스크립트
# 사용법: ./upload_to_hdfs.sh [로컬_데이터_경로]
#
# 데이터 플로우:
# (1) gruenau1 등 로컬 → hongsik1 VM (rsync)
# (2) hongsik1의 Docker 컨테이너(hdfs-namenode) 안에서 hdfs dfs -put 실행

set -euo pipefail

LOCAL_DATA_DIR=${1:-"./data/reads"}
HDFS_DATA_DIR="/genome/reads"
VM_HOST="kimhongs@hongsik1.vm.informatik.hu-berlin.de"

VM_PROJECT_DIR="~/spark-genome-pipeline"
VM_UPLOAD_DIR="${VM_PROJECT_DIR}/local-upload"
CONTAINER_UPLOAD_DIR="/tmp/hdfs-upload"

echo "=== HDFS 데이터 업로드 시작 ==="
echo "로컬 경로: ${LOCAL_DATA_DIR}"
echo "VM 호스트: ${VM_HOST}"
echo "VM 업로드 경로: ${VM_UPLOAD_DIR}"
echo "HDFS 경로: ${HDFS_DATA_DIR}"
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
echo ""

if [ ! -d "${LOCAL_DATA_DIR}" ]; then
    echo "에러: 로컬 데이터 디렉토리를 찾을 수 없습니다: ${LOCAL_DATA_DIR}" >&2
    exit 1
fi

echo "=== 1단계: 로컬 데이터 → hongsik1 동기화 ==="
ssh -o StrictHostKeyChecking=no "${VM_HOST}" "mkdir -p ${VM_UPLOAD_DIR}"

rsync -av --progress "${LOCAL_DATA_DIR}/" "${VM_HOST}:${VM_UPLOAD_DIR}/"

echo ""
echo "=== 2단계: hongsik1의 hdfs-namenode 컨테이너에서 HDFS로 업로드 ==="

# HDFS 디렉토리 생성
ssh -o StrictHostKeyChecking=no "${VM_HOST}" \
  "docker exec hdfs-namenode /opt/hadoop/bin/hdfs dfs -mkdir -p ${HDFS_DATA_DIR}"

echo "FASTQ 파일 업로드 중..."

# 컨테이너 안에서 업로드 실행 (docker cp + hdfs dfs -put)
ssh -o StrictHostKeyChecking=no "${VM_HOST}" bash -lc "
  mkdir -p \"${VM_UPLOAD_DIR}\"
  docker exec hdfs-namenode mkdir -p ${CONTAINER_UPLOAD_DIR}

  for file in ${VM_UPLOAD_DIR}/*; do
    if [ -f \"\$file\" ]; then
      base=\$(basename \"\$file\")
      echo \"컨테이너로 복사 중: \$file -> ${CONTAINER_UPLOAD_DIR}/\$base\"
      docker cp \"\$file\" hdfs-namenode:${CONTAINER_UPLOAD_DIR}/\$base
      echo \"HDFS로 업로드 중: ${CONTAINER_UPLOAD_DIR}/\$base -> ${HDFS_DATA_DIR}/\"
      docker exec hdfs-namenode /opt/hadoop/bin/hdfs dfs -put -f \"${CONTAINER_UPLOAD_DIR}/\$base\" ${HDFS_DATA_DIR}/
      docker exec hdfs-namenode rm -f \"${CONTAINER_UPLOAD_DIR}/\$base\" || true
    fi
  done
"

echo "=== 업로드 완료 ==="
ssh -o StrictHostKeyChecking=no "${VM_HOST}" \
  "docker exec hdfs-namenode /opt/hadoop/bin/hdfs dfs -ls ${HDFS_DATA_DIR} || echo 'HDFS 경로를 나열하는 데 실패했습니다.'"

echo "=== 데이터 분산 상태 ==="
ssh -o StrictHostKeyChecking=no "${VM_HOST}" \
  "docker exec hdfs-namenode /opt/hadoop/bin/hdfs dfs -du -h ${HDFS_DATA_DIR} || echo 'HDFS 사용량 확인에 실패했습니다.'"

