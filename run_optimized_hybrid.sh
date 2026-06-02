#!/bin/bash

# 최적화된 Spark Hybrid 파이프라인 실행 스크립트
# genCov와 동일한 조건으로 설정

echo "=========================================="
echo "Spark Hybrid 파이프라인 최적화 실행"
echo "genCov와 동일한 조건으로 설정"
echo "=========================================="

# 환경 변수 설정
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export SPARK_LOCAL_IP="127.0.0.1"

# 기존 결과 정리
echo "기존 결과 정리 중..."
rm -rf results/hybrid_pipeline/*
rm -rf temp/*

# 실행 시간 측정 시작
echo "실행 시작: $(date)"
start_time=$(date +%s)

# 최적화된 파이프라인 실행
time python main_hybrid.py

# 실행 시간 측정 종료
end_time=$(date +%s)
duration=$((end_time - start_time))

echo "=========================================="
echo "실행 완료: $(date)"
echo "총 실행 시간: ${duration}초 ($(($duration / 60))분 $(($duration % 60))초)"
echo "=========================================="

# 결과 확인
echo "결과 파일 확인:"
ls -la results/hybrid_pipeline/

echo "성능 비교:"
echo "- genCov (16스레드): 1분 2초"
echo "- Spark Hybrid (최적화): ${duration}초"
echo "- 성능 차이: $(echo "scale=2; ${duration} / 62" | bc)배"



