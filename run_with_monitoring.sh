#!/bin/bash

# 파이프라인 실행과 동시에 리소스 모니터링
# genCov와 Spark Hybrid의 리소스 사용량 비교

PIPELINE_TYPE=$1  # "gencov" 또는 "spark"
MONITORING_DIR="monitoring_results_$(date +%Y%m%d_%H%M%S)"

if [ -z "$PIPELINE_TYPE" ]; then
    echo "사용법: $0 [gencov|spark]"
    echo "예시: $0 gencov"
    echo "예시: $0 spark"
    exit 1
fi

echo "=========================================="
echo "파이프라인 리소스 모니터링 시작"
echo "파이프라인: $PIPELINE_TYPE"
echo "모니터링 디렉토리: $MONITORING_DIR"
echo "=========================================="

# 모니터링 디렉토리 생성
mkdir -p $MONITORING_DIR
cd $MONITORING_DIR

# 백그라운드에서 시스템 리소스 모니터링 시작
echo "시스템 리소스 모니터링 시작..."
../monitor_resources.sh > system_resources.log 2>&1 &
SYSTEM_MONITOR_PID=$!

# 백그라운드에서 프로세스별 모니터링 시작
echo "프로세스별 리소스 모니터링 시작..."
../monitor_processes.sh > process_resources.log 2>&1 &
PROCESS_MONITOR_PID=$!

# 잠시 대기 (모니터링 시작 대기)
sleep 3

# 파이프라인 실행 시간 측정 시작
echo "파이프라인 실행 시작: $(date)"
start_time=$(date +%s)

# 파이프라인 실행
    case $PIPELINE_TYPE in
        "gencov")
            echo "genCov 파이프라인 실행 중..."
            cd /vol/fob-vol7/mi21/kimhongs/genCov
            time ./nextflow run main.nf
            ;;
        "spark")
            echo "Spark Hybrid 파이프라인 실행 중..."
            cd /vol/fob-vol7/mi21/kimhongs/spark-genome-pipeline
            time python main_hybrid.py
            ;;
        *)
            echo "잘못된 파이프라인 타입: $PIPELINE_TYPE"
            exit 1
            ;;
    esac

# 파이프라인 실행 시간 측정 종료
end_time=$(date +%s)
duration=$((end_time - start_time))

echo "파이프라인 실행 완료: $(date)"
echo "총 실행 시간: ${duration}초"

# 모니터링 프로세스 종료
echo "모니터링 프로세스 종료 중..."
kill $SYSTEM_MONITOR_PID 2>/dev/null
kill $PROCESS_MONITOR_PID 2>/dev/null

# 잠시 대기 (로그 파일 완성 대기)
sleep 2

# 결과 분석
echo "=========================================="
echo "리소스 사용량 분석 결과"
echo "=========================================="

# 시스템 리소스 분석
if [ -f "system_resources.log" ]; then
    echo "시스템 리소스 사용량:"
    echo "최대 CPU 사용률: $(grep -v "Timestamp" system_resources.log | cut -d',' -f2 | sort -n | tail -1)%"
    echo "평균 CPU 사용률: $(grep -v "Timestamp" system_resources.log | cut -d',' -f2 | awk '{sum+=$1} END {print sum/NR}')%"
    echo "최대 메모리 사용량: $(grep -v "Timestamp" system_resources.log | cut -d',' -f4 | sort -n | tail -1)GB"
    echo "평균 메모리 사용량: $(grep -v "Timestamp" system_resources.log | cut -d',' -f4 | awk '{sum+=$1} END {print sum/NR}')GB"
fi

# 프로세스별 리소스 분석
if [ -f "process_resources.log" ]; then
    echo ""
    echo "프로세스별 리소스 사용량:"
    echo "fastp 최대 CPU: $(grep "fastp" process_resources.log | cut -d',' -f4 | sort -n | tail -1)%"
    echo "bwa 최대 CPU: $(grep "bwa" process_resources.log | cut -d',' -f4 | sort -n | tail -1)%"
    echo "java 최대 CPU: $(grep "java" process_resources.log | cut -d',' -f4 | sort -n | tail -1)%"
    echo "python 최대 CPU: $(grep "python" process_resources.log | cut -d',' -f4 | sort -n | tail -1)%"
fi

echo ""
echo "모니터링 결과 저장 위치: $MONITORING_DIR"
echo "시스템 리소스 로그: $MONITORING_DIR/system_resources.log"
echo "프로세스별 리소스 로그: $MONITORING_DIR/process_resources.log"
