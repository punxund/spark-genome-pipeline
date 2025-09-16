#!/bin/bash

# HDFS 클러스터 설정 스크립트
# hongsik1에서 실행 (NameNode)
# hongsik2-4에서 실행 (DataNode)

set -euo pipefail

HOSTNAME=$(hostname)
echo "=== HDFS 클러스터 설정 시작 ==="
echo "현재 호스트: $HOSTNAME"

# HDFS 데이터 디렉토리 생성
echo "=== HDFS 데이터 디렉토리 생성 ==="
sudo mkdir -p /opt/hadoop/data/namenode
sudo mkdir -p /opt/hadoop/data/datanode
sudo chown -R kimhongs:kimhongs /opt/hadoop/data
sudo chmod -R 755 /opt/hadoop/data

# HDFS 설정 파일 복사
echo "=== HDFS 설정 파일 복사 ==="
if [ -d "/workspace/pipeline/hadoop-config" ]; then
    sudo cp -r /workspace/pipeline/hadoop-config/* /opt/hadoop/etc/hadoop/
    sudo chown -R kimhongs:kimhongs /opt/hadoop/etc/hadoop
    echo "HDFS 설정 파일 복사 완료"
else
    echo "경고: hadoop-config 디렉토리를 찾을 수 없습니다."
fi

# NameNode 초기화 (hongsik1에서만)
if [[ "$HOSTNAME" == "hongsik1" ]]; then
    echo "=== NameNode 초기화 ==="
    /opt/hadoop/bin/hdfs namenode -format -force
    echo "NameNode 초기화 완료"
    
    # NameNode 시작
    echo "=== NameNode 시작 ==="
    /opt/hadoop/sbin/start-dfs.sh
    echo "NameNode 및 DataNode들 시작 완료"
    
    # HDFS 상태 확인
    echo "=== HDFS 상태 확인 ==="
    sleep 5
    /opt/hadoop/bin/hdfs dfsadmin -report
else
    echo "=== DataNode 설정 완료 ==="
    echo "NameNode에서 start-dfs.sh를 실행하면 자동으로 연결됩니다."
fi

echo "=== HDFS 클러스터 설정 완료 ==="
