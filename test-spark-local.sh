#!/bin/bash

# 로컬 파일 시스템을 사용한 Spark 파이프라인 테스트

echo "=== Spark 파이프라인 테스트 (로컬 파일 시스템) ==="

# hongsik1에서 Spark 파이프라인 테스트
ssh -o StrictHostKeyChecking=no kimhongs@hongsik1.vm.informatik.hu-berlin.de 'bash -lc "cd ~/spark-genome-pipeline && echo \"=== Spark 파이프라인 테스트 ===\" && docker run --rm -v $(pwd)/data:/workspace/data -v $(pwd):/workspace ec6a7cf0a97a bash -c \"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/opt/spark
export PATH=\$SPARK_HOME/bin:\$PATH

# Python 환경 확인
echo \"=== Python 환경 확인 ===\"
python3 --version
pip3 list | grep -E \"(pysam|pybedtools|pyarrow|pandas)\"

# 데이터 파일 확인
echo \"=== 데이터 파일 확인 ===\"
ls -la /workspace/data/
ls -la /workspace/data/reads/

# Spark 버전 확인
echo \"=== Spark 버전 확인 ===\"
spark-submit --version

# 간단한 Spark 테스트
echo \"=== Spark 테스트 ===\"
spark-submit --master local[2] --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true /opt/spark/examples/src/main/python/pi.py 10

# main_hybrid.py 파일 확인
echo \"=== main_hybrid.py 파일 확인 ===\"
ls -la /workspace/main_hybrid.py
head -20 /workspace/main_hybrid.py
\""'

echo "Spark 파이프라인 테스트 완료!"



