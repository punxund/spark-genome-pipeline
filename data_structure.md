# 데이터 디렉토리 구조

## 필수 디렉토리 구조

프로젝트 루트에서 다음과 같은 디렉토리 구조가 필요합니다:

```
genCov/
├── spark_genome_pipeline/     # 이 프로젝트
│   ├── main.py
│   ├── config.py
│   ├── requirements.txt
│   └── ...
├── data/                      # 입력 데이터
│   ├── reads/                 # FASTQ 파일들
│   │   ├── SRR30977596_1.fastq
│   │   └── SRR30977596_2.fastq
│   └── ref_sequence_genB.fa   # 참조 게놈
├── results/                   # 결과 파일들 (자동 생성)
│   └── spark_pipeline/
└── temp/                      # 임시 파일들 (자동 생성)
```

## 입력 파일 요구사항

### FASTQ 파일
- **위치**: `../data/reads/`
- **파일명 형식**: `{sample_id}_1.fastq`, `{sample_id}_2.fastq`
- **예시**: `SRR30977596_1.fastq`, `SRR30977596_2.fastq`

### 참조 게놈
- **위치**: `../data/ref_sequence_genB.fa`
- **형식**: FASTA
- **인덱스**: `../data/ref_sequence_genB.fa.fai` (자동 생성됨)

## 실행 방법

1. **데이터 준비**:
   ```bash
   mkdir -p ../data/reads
   # FASTQ 파일들을 ../data/reads/에 복사
   # 참조 게놈을 ../data/ref_sequence_genB.fa로 복사
   ```

2. **파이프라인 실행**:
   ```bash
   python3 main.py
   ```

3. **결과 확인**:
   ```bash
   ls ../results/spark_pipeline/
   ```

## 설정 변경

`config.py`에서 경로를 수정할 수 있습니다:

```python
# 데이터 경로
DATA_DIR = Path("../data")
READS_DIR = DATA_DIR / "reads"
REFERENCE_GENOME = DATA_DIR / "ref_sequence_genB.fa"
```
