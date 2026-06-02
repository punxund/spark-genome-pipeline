#!/bin/bash

# 성능 최적화 환경변수 설정
echo "성능 최적화 환경변수 설정 중..."

# NumExpr 스레드 수 최대화
export NUMEXPR_MAX_THREADS=72

# Spark 로컬 디렉토리 설정 (로컬 디스크 사용)
export SPARK_LOCAL_DIRS=/tmp
export TMPDIR=/tmp

# Python 최적화
export PYTHONOPTIMIZE=2
export PYTHONUNBUFFERED=1

# 메모리 최적화
export MALLOC_ARENA_MAX=2

# 파일 시스템 최적화
export OMP_NUM_THREADS=72
export OPENBLAS_NUM_THREADS=72
export MKL_NUM_THREADS=72
export VECLIB_MAXIMUM_THREADS=72

# Spark 추가 최적화
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_PUBLIC_DNS=localhost

echo "환경변수 설정 완료:"
echo "  - NUMEXPR_MAX_THREADS: $NUMEXPR_MAX_THREADS"
echo "  - SPARK_LOCAL_DIRS: $SPARK_LOCAL_DIRS"
echo "  - OMP_NUM_THREADS: $OMP_NUM_THREADS"
echo "  - PYTHONOPTIMIZE: $PYTHONOPTIMIZE"

echo ""
echo "이제 파이프라인을 실행하세요:"
echo "python main.py --reads-dir data/reads --reference-genome data/ref_sequence_genB.fa"

