#!/bin/bash

# Spark 파이프라인 테스트 스크립트

echo "=== Spark 파이프라인 테스트 ==="

docker run --rm -v $(pwd)/data:/workspace/data -v $(pwd)/output:/workspace/output ec6a7cf0a97a bash -c "
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

# Spark 마스터 시작
echo \"=== Spark 마스터 시작 ===\"
/opt/spark/sbin/start-master.sh

# 잠시 대기
sleep 5

# Spark 워커 시작
echo \"=== Spark 워커 시작 ===\"
/opt/spark/sbin/start-worker.sh spark://localhost:7077

# 잠시 대기
sleep 5

# Spark 상태 확인
echo \"=== Spark 상태 확인 ===\"
/opt/spark/bin/spark-submit --version

# 간단한 Spark 애플리케이션 테스트
echo \"=== Spark 애플리케이션 테스트 ===\"
/opt/spark/bin/spark-submit --master spark://localhost:7077 --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 10

# Spark 마스터와 워커 종료
echo \"=== Spark 종료 ===\"
/opt/spark/sbin/stop-worker.sh
/opt/spark/sbin/stop-master.sh
"

echo "Spark 파이프라인 테스트 완료!"



