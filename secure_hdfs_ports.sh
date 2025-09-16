#!/bin/bash

# HDFS 포트 보안 강화 스크립트
# VM들 간 통신과 gruenau1에서만 접근 허용

set -euo pipefail

# 네트워크 설정
INTERNAL_NETWORK="141.20.38.0/24"  # VM들 간 통신
GRUENAU1_IP="141.20.38.1"          # gruenau1 IP (게이트웨이)

echo "=== HDFS 포트 보안 강화 ==="
echo "내부 네트워크: $INTERNAL_NETWORK"
echo "Gruenau1 IP: $GRUENAU1_IP"
echo ""

# 기존 Anywhere 규칙 삭제
echo "기존 Anywhere 규칙 삭제 중..."
sudo ufw delete allow 9000/tcp
sudo ufw delete allow 9870/tcp  
sudo ufw delete allow 9868/tcp
sudo ufw delete allow 9864/tcp
sudo ufw delete allow 50070/tcp
sudo ufw delete allow 50075/tcp

# 보안 강화된 규칙 추가
echo "보안 강화된 규칙 추가 중..."

# HDFS NameNode RPC (9000) - VM들 간 통신만
sudo ufw allow from $INTERNAL_NETWORK to any port 9000 comment "HDFS NameNode RPC (internal only)"

# HDFS NameNode HTTP (9870) - VM들 간 + gruenau1
sudo ufw allow from $INTERNAL_NETWORK to any port 9870 comment "HDFS NameNode HTTP (internal)"
sudo ufw allow from $GRUENAU1_IP to any port 9870 comment "HDFS NameNode HTTP (gruenau1)"

# HDFS Secondary NameNode (9868) - VM들 간 통신만
sudo ufw allow from $INTERNAL_NETWORK to any port 9868 comment "HDFS Secondary NameNode (internal only)"

# HDFS DataNode (9864) - VM들 간 통신만
sudo ufw allow from $INTERNAL_NETWORK to any port 9864 comment "HDFS DataNode (internal only)"

# Legacy 포트들 (필요시)
sudo ufw allow from $INTERNAL_NETWORK to any port 50070 comment "HDFS NameNode HTTP legacy (internal)"
sudo ufw allow from $INTERNAL_NETWORK to any port 50075 comment "HDFS DataNode HTTP legacy (internal)"

echo ""
echo "=== UFW 상태 확인 ==="
sudo ufw status

echo ""
echo "=== 보안 규칙 요약 ==="
echo "9000 (NameNode RPC): VM들 간 통신만"
echo "9870 (NameNode HTTP): VM들 간 + gruenau1"
echo "9868 (Secondary NameNode): VM들 간 통신만"
echo "9864 (DataNode): VM들 간 통신만"
echo "50070/50075 (Legacy): VM들 간 통신만"
