# Spark 유전체 분석 파이프라인

이 프로젝트는 Apache Spark를 사용하여 유전체 분석 파이프라인을 구현한 것입니다. 기존의 Nextflow 파이프라인을 Spark로 포팅하여 대규모 데이터 처리와 병렬 처리를 지원합니다.

## 개요

이 파이프라인은 다음 4단계로 구성되어 있습니다:

1. **전처리 (fastp)**: FASTQ 파일 정리 및 품질 제어
2. **읽기 매핑 (BWA)**: 참조 게놈에 읽기 정렬
3. **SAM 처리 (samtools)**: BAM 파일 정렬 및 통계 생성
4. **커버리지 계산 (bedtools)**: 게놈 커버리지 분석

## 특징

- **병렬 처리**: Spark를 사용한 대규모 데이터 병렬 처리
- **확장성**: 클러스터 환경에서 실행 가능
- **모듈화**: 각 단계가 독립적인 모듈로 구성
- **오류 처리**: 각 단계별 상세한 오류 처리 및 로깅
- **결과 관리**: JSON 형태의 상세한 결과 보고서 생성

## 요구사항

### 시스템 요구사항
- Python 3.8+
- Apache Spark 3.5.0+
- Java 8+ (Spark 실행을 위해)

### 생물정보학 도구
- fastp (FASTQ 전처리)
- BWA (읽기 매핑)
- samtools (SAM/BAM 처리)
- bedtools (커버리지 계산)
- bedGraphToBigWig (BigWig 파일 생성, 선택사항)

### Python 패키지
```
pyspark==3.5.0
pandas==2.1.4
numpy==1.24.3
pybedtools==0.9.0
pyBigWig==0.3.22
matplotlib==3.8.2
seaborn==0.13.0
plotly==5.17.0
fastq-pair==1.0.0
pysam==0.21.0
```

## 설치

1. 저장소 클론:
```bash
git clone <repository-url>
cd spark_genome_pipeline
```

2. Python 가상환경 생성 및 활성화:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 또는
venv\Scripts\activate  # Windows
```

3. 의존성 설치:
```bash
pip install -r requirements.txt
```

4. 생물정보학 도구 설치:
```bash
# Ubuntu/Debian
sudo apt-get install fastp bwa samtools bedtools

# CentOS/RHEL
sudo yum install fastp bwa samtools bedtools

# 또는 conda 사용
conda install -c bioconda fastp bwa samtools bedtools
```

## 사용법

### 기본 실행

```bash
python main.py
```

### 고급 옵션

```bash
python main.py \
    --reads-dir /path/to/reads \
    --reference-genome /path/to/reference.fa \
    --reference-index /path/to/reference.fa.fai \
    --spark-master local[*]
```

### 매개변수

- `--reads-dir`: FASTQ 파일들이 있는 디렉토리 (기본값: `../data/reads`)
- `--reference-genome`: 참조 게놈 FASTA 파일 (기본값: `../data/ref_sequence_genB.fa`)
- `--reference-index`: 참조 게놈 인덱스 파일 (기본값: `../data/ref_sequence_genB.fa.fai`)
- `--spark-master`: Spark 마스터 URL (기본값: `local[*]`)

## 입력 데이터 형식

### FASTQ 파일
- 파일명 형식: `{sample_id}_1.fastq`, `{sample_id}_2.fastq`
- 예: `SRR30977596_1.fastq`, `SRR30977596_2.fastq`

### 참조 게놈
- FASTA 형식
- 인덱스 파일 (.fai) 필요

## 출력 결과

### 파일 구조
```
results/spark_pipeline/
├── *.R1.trimmed.fastq          # 전처리된 R1 파일
├── *.R2.trimmed.fastq          # 전처리된 R2 파일
├── report_*_fastp.json         # fastp JSON 보고서
├── report_*_fastp.html         # fastp HTML 보고서
├── *.sam                       # BWA 매핑 결과
├── *_filtered.bam              # 정렬된 BAM 파일
├── *_mapping_stats.txt         # 매핑 통계
├── *_coverage.bed              # 커버리지 BED 파일
├── *.bw                        # BigWig 파일 (선택사항)
├── pipeline_results.json       # 전체 파이프라인 결과
├── pipeline_summary.json       # 요약 보고서
├── coverage_analysis_plots.png # 커버리지 분석 플롯
└── pipeline.log                # 실행 로그
```

### 보고서 파일

#### pipeline_results.json
전체 파이프라인 실행 결과를 포함하는 상세한 JSON 파일

#### pipeline_summary.json
각 단계별 성공/실패 통계와 실행 시간 요약

#### coverage_analysis_plots.png
커버리지 분석 결과를 시각화한 플롯

## 설정

`config.py` 파일에서 다음 설정들을 변경할 수 있습니다:

- 데이터 경로
- Spark 설정
- 도구 경로
- 메모리 설정
- 파티션 크기

## 클러스터 실행

Spark 클러스터에서 실행하려면:

```bash
spark-submit \
    --master spark://your-cluster:7077 \
    --driver-memory 4g \
    --executor-memory 4g \
    --executor-cores 2 \
    main.py \
    --reads-dir /path/to/reads \
    --reference-genome /path/to/reference.fa
```
## 성능 최적화

### 메모리 설정
- 대용량 데이터의 경우 `--driver-memory`와 `--executor-memory` 증가
- 클러스터 환경에서는 각 노드의 메모리에 맞게 조정

### 파티션 설정
- `config.py`의 `PARTITION_SIZE` 조정으로 파티션 크기 최적화
- 데이터 크기와 클러스터 리소스에 따라 조정

## 문제 해결

### 일반적인 오류

1. **도구를 찾을 수 없음**
   - PATH 환경변수 확인
   - 도구 설치 상태 확인

2. **메모리 부족**
   - Spark 메모리 설정 증가
   - 파티션 크기 조정

3. **권한 오류**
   - 결과 디렉토리 쓰기 권한 확인
   - 임시 파일 디렉토리 권한 확인

### 로그 확인
- `results/spark_pipeline/pipeline.log`에서 상세한 오류 정보 확인

## 기여

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 연락처

문의사항이나 버그 리포트는 이슈를 통해 제출해 주세요.

