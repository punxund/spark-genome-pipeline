#!/usr/bin/env python3
"""
Minimap2를 사용한 Spark 유전체 분석 파이프라인

이 패키지는 효율적인 외부 도구들을 사용합니다:
- minimap2: 빠른 읽기 매핑
- samtools: SAM/BAM 파일 처리
- pybigwig: BigWig 파일 생성
- numpy: 수치 계산
- pandas: 데이터 처리
"""

from .config_pytools import PyToolsConfig
from .pytools_preprocessing import run_preprocessing, PyToolsPreprocessor
from .pytools_alignment import run_alignment, PyToolsAligner
from .pytools_sam_processing import run_sam_processing, PyToolsSAMProcessor
from .pytools_coverage import run_coverage_calculation, PyToolsCoverageCalculator

__version__ = "1.0.0"
__author__ = "Spark Genome Pipeline Team"

__all__ = [
    "PyToolsConfig",
    "run_preprocessing", 
    "PyToolsPreprocessor",
    "run_alignment",
    "PyToolsAligner",
    "run_sam_processing",
    "PyToolsSAMProcessor",
    "run_coverage_calculation",
    "PyToolsCoverageCalculator"
]
