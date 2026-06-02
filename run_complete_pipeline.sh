#!/bin/bash

# 완전한 GenCov Pipeline 실행 스크립트
# HDFS → Spark → GenCov Pipeline 순서로 실행

set -euo pipefail

echo "=== GenCov Pipeline 완전 실행 시작 ==="
echo "실행 시간: $(date)"

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수들
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 1단계: HDFS 클러스터 구성
log_info "=== 1단계: HDFS 클러스터 구성 ==="
if [ -f "./setup-hdfs-cluster.sh" ]; then
    log_info "HDFS 클러스터 설정 시작..."
    ./setup-hdfs-cluster.sh
    log_success "HDFS 클러스터 구성 완료"
else
    log_error "setup-hdfs-cluster.sh 파일을 찾을 수 없습니다"
    exit 1
fi

# HDFS 상태 확인
log_info "HDFS 클러스터 상태 확인..."
sleep 10
ssh kimhongs@hongsik1.vm.informatik.hu-berlin.de 'docker exec $(docker ps -q --filter "ancestor=ec6a7cf0a97a") /opt/hadoop/bin/hdfs dfsadmin -report' || log_warning "HDFS 상태 확인 실패"

# 2단계: Spark 클러스터 구성
log_info "=== 2단계: Spark 클러스터 구성 ==="

# Docker 이미지 빌드 (hongsik1에서)
log_info "Spark Docker 이미지 빌드..."
ssh kimhongs@hongsik1.vm.informatik.hu-berlin.de 'cd ~/spark-genome-pipeline && docker build -t spark-bio:latest .'
log_success "Docker 이미지 빌드 완료"

# Spark Master 시작 (hongsik1에서)
log_info "Spark Master 시작..."
ssh kimhongs@hongsik1.vm.informatik.hu-berlin.de 'cd ~/spark-genome-pipeline && docker compose -f docker-compose.master.yml up -d'
log_success "Spark Master 시작 완료"

# Spark Workers 시작 (hongsik2,3,4에서)
log_info "Spark Workers 시작..."
for vm in hongsik2 hongsik3 hongsik4; do
    log_info "$vm에서 Spark Worker 시작..."
    ssh kimhongs@${vm}.vm.informatik.hu-berlin.de 'cd ~/spark-genome-pipeline && docker compose -f docker-compose.worker.yml up -d'
    log_success "$vm Spark Worker 시작 완료"
done

# Spark 클러스터 상태 확인
log_info "Spark 클러스터 상태 확인..."
sleep 15
curl -s http://141.20.38.81:8080 | grep -q "Spark Master" && log_success "Spark Master WebUI 접근 가능" || log_warning "Spark Master WebUI 접근 실패"

# 3단계: GenCov Pipeline 실행
log_info "=== 3단계: GenCov Pipeline 실행 ==="

# 파이프라인 실행
log_info "하이브리드 GenCov 파이프라인 시작..."
ssh kimhongs@hongsik1.vm.informatik.hu-berlin.de 'cd ~/spark-genome-pipeline && docker exec -it spark-master bash -lc "
  spark-submit \
  --master spark://141.20.38.81:7077 \
  --deploy-mode client \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=6 \
  --conf spark.driver.memory=4g \
  --conf spark.sql.adaptive.enabled=true \
  /workspace/pipeline/main_hybrid.py \
  --reads-dir /mnt/genome/reads \
  --reference-genome /mnt/genome/refs/ref.fa \
  --reference-index /mnt/genome/refs/ref.fa.fai \
  --spark-master spark://141.20.38.81:7077
"'

log_success "GenCov Pipeline 실행 완료"

# 최종 상태 확인
log_info "=== 최종 상태 확인 ==="
log_info "HDFS WebUI: http://141.20.38.81:9870"
log_info "Spark Master WebUI: http://141.20.38.81:8080"
log_info "결과 디렉토리: /mnt/genome/results"

echo "=== GenCov Pipeline 완전 실행 완료 ==="
echo "완료 시간: $(date)"
