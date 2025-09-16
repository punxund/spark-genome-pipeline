# Spark 유전체 분석 파이프라인 - Hybrid 버전

## 개요

이 프로젝트는 Spark를 기반으로 한 유전체 분석 파이프라인의 Hybrid 버전입니다. 기존의 외부 도구들과 Python 라이브러리들을 조합하여 효율적인 유전체 분석을 수행합니다.

## 특징

### Hybrid 접근 방식
- **전처리**: fastp (외부 도구) - 빠른 FASTQ 처리
- **매핑**: BWA (외부 도구) - 정확한 읽기 정렬
- **SAM 처리**: pysam (Python 라이브러리) - 효율적인 SAM/BAM 처리
- **커버리지**: pybedtools + pybigwig (Python 라이브러리) - 유연한 커버리지 분석

### 장점
1. **성능 최적화**: 각 단계에서 최적의 도구 사용
2. **Python 통합**: Python 라이브러리로 더 세밀한 제어 가능
3. **확장성**: Spark 기반으로 대용량 데이터 처리 가능
4. **유연성**: 필요에 따라 외부 도구와 Python 라이브러리 조합 가능

## 설치

### 1. 시스템 요구사항
- Python 3.8+
- Apache Spark 3.4+
- Java 8+

### 2. 외부 도구 설치
```bash
# fastp 설치
conda install -c bioconda fastp

# BWA 설치
conda install -c bioconda bwa
```

### 3. Python 라이브러리 설치
```bash
pip install -r requirements_hybrid.txt
```

### 4. 필수 Python 라이브러리
- `pysam>=0.21.0`: SAM/BAM 파일 처리
- `pybedtools>=0.9.0`: BED 파일 처리 및 커버리지 계산
- `pybigwig>=0.3.18`: BigWig 파일 생성/읽기

## 사용법

### 1. 기본 실행
```bash
./run_hybrid_pipeline.sh \
    -r /path/to/reads \
    -g /path/to/reference.fa \
    -i /path/to/reference.fa.fai
```

### 2. Python 직접 실행
```bash
python3 main_hybrid.py \
    --reads-dir /path/to/reads \
    --reference-genome /path/to/reference.fa \
    --reference-index /path/to/reference.fa.fai \
    --spark-master local[*]
```

### 3. Spark 클러스터에서 실행
```bash
python3 main_hybrid.py \
    --reads-dir /path/to/reads \
    --reference-genome /path/to/reference.fa \
    --reference-index /path/to/reference.fa.fai \
    --spark-master spark://your-cluster:7077
```

## 파이프라인 단계

### 1단계: FASTQ 전처리 (fastp)
- 품질 필터링
- 어댑터 제거
- 리드 트리밍

### 2단계: 읽기 매핑 (BWA)
- 참조 게놈에 읽기 정렬
- SAM 파일 생성

### 3단계: SAM 처리 (pysam)
- 품질 필터링 (MAPQ >= 20)
- 중복 제거 (선택적)
- BAM 파일 생성 및 인덱싱
- 매핑 통계 계산

### 4단계: 커버리지 계산 (pybedtools + pybigwig)
- 게놈 커버리지 계산
- 윈도우 기반 커버리지 분석
- BED 파일 생성
- BigWig 파일 생성

## 출력 파일

### 결과 디렉토리 구조
```
results/
├── preprocessed_results.parquet
├── alignment_results.parquet
├── sam_processed_results.parquet
├── coverage_results.parquet
├── pipeline_hybrid_results.json
├── pipeline_hybrid_summary.json
├── pipeline_hybrid.log
├── sample1_filtered.bam
├── sample1_filtered.bam.bai
├── sample1_coverage.bed
├── sample1.bw
├── sample1_pysam_stats.json
└── sample1_pybedtools_coverage_stats.json
```

### 주요 출력 파일
- **BAM 파일**: 필터링된 정렬 결과
- **BED 파일**: 커버리지 데이터
- **BigWig 파일**: 시각화용 커버리지 데이터
- **통계 파일**: 각 단계별 처리 통계
- **Parquet 파일**: Spark DataFrame 결과

## 설정

### config_pytools.py 설정
```python
# SAM 처리 설정
SAM_PROCESSING_CONFIG = {
    "filter_quality": 20,      # 최소 매핑 품질
    "remove_duplicates": True  # 중복 제거 여부
}

# 커버리지 설정
COVERAGE_CONFIG = {
    "window_size": 1000        # 커버리지 윈도우 크기
}
```

## 성능 최적화

### Spark 설정
```python
# Spark 설정 예시
SPARK_CONFIG = {
    "spark.app.name": "genome-analysis-hybrid",
    "spark.master": "local[*]",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.sql.adaptive.enabled": "true"
}
```

### 메모리 최적화
- BAM 파일 처리 시 청크 단위로 처리
- 커버리지 계산 시 윈도우 크기 조정
- Spark 파티션 수 조정

## 문제 해결

### 일반적인 오류

1. **pysam ImportError**
   ```bash
   pip install pysam
   ```

2. **pybedtools ImportError**
   ```bash
   pip install pybedtools
   ```

3. **pybigwig ImportError**
   ```bash
   pip install pybigwig
   ```

4. **메모리 부족**
   - Spark 메모리 설정 증가
   - 윈도우 크기 감소
   - 파티션 수 조정

### 로그 확인
```bash
tail -f results/pipeline_hybrid.log
```

## 기존 파이프라인과의 비교

| 기능 | 기존 파이프라인 | Hybrid 파이프라인 |
|------|----------------|-------------------|
| SAM 처리 | samtools (외부) | pysam (Python) |
| 커버리지 | bedtools (외부) | pybedtools (Python) |
| BigWig | bedGraphToBigWig (외부) | pybigwig (Python) |
| 성능 | 안정적 | 최적화됨 |
| 유연성 | 제한적 | 높음 |
| 디버깅 | 어려움 | 쉬움 |

## 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 기여

버그 리포트, 기능 요청, 풀 리퀘스트를 환영합니다.

## 연락처

프로젝트 관련 문의사항이 있으시면 이슈를 생성해 주세요.








