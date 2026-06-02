#!/bin/bash

# 리소스 사용량 간단 분석 스크립트

GENCOV_LOG="monitoring_results_20250820_104912/system_resources.log"
SPARK_LOG="monitoring_results_20250820_105321/system_resources.log"

echo "=========================================="
echo "리소스 사용량 분석 결과"
echo "=========================================="

# genCov 분석
if [ -f "$GENCOV_LOG" ]; then
    echo "genCov 파이프라인 리소스 사용량:"
    echo "----------------------------------------"
    
    # CPU 사용률 분석
    CPU_VALUES=$(grep -v "Timestamp\|시스템\|측정\|==" "$GENCOV_LOG" | cut -d',' -f2 | grep -v "^$" | sort -n)
    if [ ! -z "$CPU_VALUES" ]; then
        MAX_CPU=$(echo "$CPU_VALUES" | tail -1)
        AVG_CPU=$(echo "$CPU_VALUES" | awk '{sum+=$1} END {print sum/NR}')
        echo "CPU 사용률 - 최대: ${MAX_CPU}%, 평균: ${AVG_CPU}%"
    fi
    
    # 메모리 사용량 분석
    MEM_VALUES=$(grep -v "Timestamp\|시스템\|측정\|==" "$GENCOV_LOG" | cut -d',' -f4 | grep -v "^$" | sort -n)
    if [ ! -z "$MEM_VALUES" ]; then
        MAX_MEM=$(echo "$MEM_VALUES" | tail -1)
        AVG_MEM=$(echo "$MEM_VALUES" | awk '{sum+=$1} END {print sum/NR}')
        echo "메모리 사용량 - 최대: ${MAX_MEM}GB, 평균: ${AVG_MEM}GB"
    fi
    
    # 시스템 로드 분석
    LOAD_VALUES=$(grep -v "Timestamp\|시스템\|측정\|==" "$GENCOV_LOG" | cut -d',' -f9 | grep -v "^$" | sort -n)
    if [ ! -z "$LOAD_VALUES" ]; then
        MAX_LOAD=$(echo "$LOAD_VALUES" | tail -1)
        AVG_LOAD=$(echo "$LOAD_VALUES" | awk '{sum+=$1} END {print sum/NR}')
        echo "시스템 로드 - 최대: ${MAX_LOAD}, 평균: ${AVG_LOAD}"
    fi
    
    echo ""
else
    echo "genCov 로그 파일을 찾을 수 없습니다: $GENCOV_LOG"
fi

# Spark Hybrid 분석
if [ -f "$SPARK_LOG" ]; then
    echo "Spark Hybrid 파이프라인 리소스 사용량:"
    echo "----------------------------------------"
    
    # CPU 사용률 분석
    CPU_VALUES=$(grep -v "Timestamp\|시스템\|측정\|==" "$SPARK_LOG" | cut -d',' -f2 | grep -v "^$" | sort -n)
    if [ ! -z "$CPU_VALUES" ]; then
        MAX_CPU=$(echo "$CPU_VALUES" | tail -1)
        AVG_CPU=$(echo "$CPU_VALUES" | awk '{sum+=$1} END {print sum/NR}')
        echo "CPU 사용률 - 최대: ${MAX_CPU}%, 평균: ${AVG_CPU}%"
    fi
    
    # 메모리 사용량 분석
    MEM_VALUES=$(grep -v "Timestamp\|시스템\|측정\|==" "$SPARK_LOG" | cut -d',' -f4 | grep -v "^$" | sort -n)
    if [ ! -z "$MEM_VALUES" ]; then
        MAX_MEM=$(echo "$MEM_VALUES" | tail -1)
        AVG_MEM=$(echo "$MEM_VALUES" | awk '{sum+=$1} END {print sum/NR}')
        echo "메모리 사용량 - 최대: ${MAX_MEM}GB, 평균: ${AVG_MEM}GB"
    fi
    
    # 시스템 로드 분석
    LOAD_VALUES=$(grep -v "Timestamp\|시스템\|측정\|==" "$SPARK_LOG" | cut -d',' -f9 | grep -v "^$" | sort -n)
    if [ ! -z "$LOAD_VALUES" ]; then
        MAX_LOAD=$(echo "$LOAD_VALUES" | tail -1)
        AVG_LOAD=$(echo "$LOAD_VALUES" | awk '{sum+=$1} END {print sum/NR}')
        echo "시스템 로드 - 최대: ${MAX_LOAD}, 평균: ${AVG_LOAD}"
    fi
    
    echo ""
else
    echo "Spark Hybrid 로그 파일을 찾을 수 없습니다: $SPARK_LOG"
fi

# 프로세스별 분석
echo "프로세스별 리소스 사용량:"
echo "----------------------------------------"

# genCov 프로세스 분석
if [ -f "monitoring_results_20250820_104912/process_resources.log" ]; then
    echo "genCov 프로세스:"
    grep -E "(fastp|bwa|samtools|nextflow)" monitoring_results_20250820_104912/process_resources.log | tail -5
    echo ""
fi

# Spark 프로세스 분석
if [ -f "monitoring_results_20250820_105321/process_resources.log" ]; then
    echo "Spark 프로세스:"
    grep -E "(fastp|bwa|java|python|spark)" monitoring_results_20250820_105321/process_resources.log | tail -5
    echo ""
fi

echo "=========================================="
echo "분석 완료!"
echo "=========================================="



