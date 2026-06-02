#!/bin/bash

# HDFS 포트 열기 스크립트
# 각 VM에서 실행해야 합니다

set -euo pipefail

echo "=== HDFS 포트 열기 ==="
echo "다음 포트들을 열어야 합니다:"
echo "  - 9000: NameNode RPC"
echo "  - 9870: NameNode HTTP UI"
echo "  - 9868: Secondary NameNode"
echo "  - 9864: DataNode"
echo "  - 50070: NameNode HTTP (구버전)"
echo "  - 50075: DataNode HTTP (구버전)"
echo ""

# UFW로 포트 열기
echo "UFW로 포트 열기 중..."
sudo ufw allow 9000/tcp comment "HDFS NameNode RPC"
sudo ufw allow 9870/tcp comment "HDFS NameNode HTTP"
sudo ufw allow 9868/tcp comment "HDFS Secondary NameNode"
sudo ufw allow 9864/tcp comment "HDFS DataNode"
sudo ufw allow 50070/tcp comment "HDFS NameNode HTTP (legacy)"
sudo ufw allow 50075/tcp comment "HDFS DataNode HTTP (legacy)"

echo ""
echo "=== UFW 상태 확인 ==="
sudo ufw status

echo ""
echo "=== 포트 연결 테스트 ==="
echo "9000 (NameNode RPC): $(nc -z localhost 9000 && echo "열림" || echo "닫힘")"
echo "9870 (NameNode HTTP): $(nc -z localhost 9870 && echo "열림" || echo "닫힘")"
echo "9868 (Secondary NameNode): $(nc -z localhost 9868 && echo "열림" || echo "닫힘")"
echo "9864 (DataNode): $(nc -z localhost 9864 && echo "열림" || echo "닫힘")"




