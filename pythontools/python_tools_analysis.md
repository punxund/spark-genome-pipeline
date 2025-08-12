# Spark 유전체 분석 파이프라인 - Python 도구 대체 방안 분석

## 개요

현재 `main.py`에서 사용되는 외부 도구들을 Python 라이브러리로 대체하는 방안을 분석하고 제시합니다.

## 현재 사용되는 외부 도구들

### 1. fastp (FASTQ 전처리)
- **용도**: FASTQ 파일의 품질 필터링, 어댑터 제거, 리드 트리밍
- **현재 구현**: `preprocessing.py`에서 subprocess로 실행

### 2. BWA (Burrows-Wheeler Aligner)
- **용도**: 읽기를 참조 게놈에 매핑
- **현재 구현**: `alignment.py`에서 subprocess로 실행

### 3. samtools
- **용도**: SAM/BAM 파일 처리, 정렬, 통계 생성
- **현재 구현**: `sam_processing.py`에서 subprocess로 실행

### 4. bedtools
- **용도**: 게놈 커버리지 계산, BED 파일 처리
- **현재 구현**: `coverage.py`에서 subprocess로 실행

### 5. bedGraphToBigWig
- **용도**: BED 파일을 BigWig 형식으로 변환
- **현재 구현**: `coverage.py`에서 subprocess로 실행

## Python 라이브러리 대체 방안

### 1. fastp → Python 라이브러리 대체

#### 대체 가능한 라이브러리들:
- **Biopython**: FASTQ 파일 읽기/쓰기, 기본적인 품질 필터링
- **cutadapt**: 어댑터 제거 (Python 라이브러리)
- **fastqc**: 품질 통계 (Python 래퍼)

#### 구현 예시:
```python
from Bio import SeqIO
import numpy as np

def process_fastq_with_biopython(input_file, output_file):
    with open(output_file, 'w') as out:
        for record in SeqIO.parse(input_file, "fastq"):
            # 품질 필터링 (Q20 이상)
            if np.mean(record.letter_annotations["phred_quality"]) >= 20:
                # 어댑터 트리밍
                trimmed_seq = trim_adapters(record)
                if len(trimmed_seq) >= 50:  # 최소 길이 필터
                    SeqIO.write(trimmed_seq, out, "fastq")
```

#### 장점:
- 외부 의존성 제거
- Python 내에서 직접 처리 가능
- 더 세밀한 제어 가능

#### 단점:
- fastp보다 성능이 느릴 수 있음
- 복잡한 어댑터 탐지 알고리즘 구현 필요

### 2. BWA → Python 라이브러리 대체

#### 대체 가능한 라이브러리들:
- **pysam**: SAM/BAM 파일 처리
- **Biopython**: 기본적인 시퀀스 매칭
- **custom implementation**: 간단한 exact match

#### 구현 예시:
```python
from Bio import SeqIO

def simple_sequence_matching(reads_file, reference_genome):
    # 참조 게놈 로드
    ref_sequences = {}
    for record in SeqIO.parse(reference_genome, "fasta"):
        ref_sequences[record.id] = str(record.seq)
    
    # 간단한 exact match (실제 BWA는 매우 복잡한 알고리즘)
    for read in SeqIO.parse(reads_file, "fastq"):
        seq = str(read.seq)
        for ref_id, ref_seq in ref_sequences.items():
            if seq in ref_seq:
                # 매칭 결과 생성
                pass
```

#### 장점:
- 외부 의존성 제거
- Python 내에서 직접 처리

#### 단점:
- **BWA의 복잡한 알고리즘을 완전히 대체하기 어려움**
- 성능이 크게 떨어질 수 있음
- 실제 생산 환경에서는 권장하지 않음

### 3. samtools → pysam

#### 대체 가능한 라이브러리:
- **pysam**: samtools의 Python 인터페이스

#### 구현 예시:
```python
import pysam

def process_sam_with_pysam(sam_file, bam_file):
    # SAM을 BAM으로 변환
    with pysam.AlignmentFile(sam_file, "r") as sam_in:
        with pysam.AlignmentFile(bam_file, "wb", header=sam_in.header) as bam_out:
            for read in sam_in:
                # 품질 필터링 (MAPQ >= 20)
                if read.mapping_quality >= 20:
                    bam_out.write(read)
    
    # BAM 정렬
    pysam.sort("-o", "sorted.bam", bam_file)
```

#### 장점:
- **완전한 대체 가능**
- samtools와 동일한 기능 제공
- Python 내에서 직접 처리

#### 단점:
- 없음 (pysam이 samtools의 공식 Python 인터페이스)

### 4. bedtools → pybedtools

#### 대체 가능한 라이브러리:
- **pybedtools**: bedtools의 Python 인터페이스

#### 구현 예시:
```python
import pybedtools

def calculate_coverage_with_pybedtools(bam_file):
    # pybedtools를 사용한 커버리지 계산
    bam = pybedtools.BedTool(bam_file)
    coverage = bam.genome_coverage(bg=True)
    return coverage
```

#### 장점:
- **완전한 대체 가능**
- bedtools와 동일한 기능 제공
- Python 내에서 직접 처리

#### 단점:
- 없음 (pybedtools가 bedtools의 공식 Python 인터페이스)

### 5. bedGraphToBigWig → pybigwig

#### 대체 가능한 라이브러리:
- **pybigwig**: BigWig 파일 생성/읽기

#### 구현 예시:
```python
import pybigwig

def create_bigwig_from_bed(bed_file, chrom_sizes, bigwig_file):
    # BigWig 파일 생성
    bw = pybigwig.BigWigFile(bigwig_file, 'w')
    
    # 헤더 작성
    bw.addHeader(list(chrom_sizes.items()))
    
    # 커버리지 데이터 추가
    with open(bed_file, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            chrom, start, end, coverage_val = parts[0], int(parts[1]), int(parts[2]), float(parts[3])
            bw.addEntries([chrom], [start], [end], [coverage_val])
    
    bw.close()
```

#### 장점:
- **완전한 대체 가능**
- BigWig 파일 직접 생성 가능
- Python 내에서 직접 처리

#### 단점:
- 없음

## 권장 대체 전략

### 1. 완전 대체 가능한 도구들 (우선 대체)
- **samtools → pysam**: 완전 대체 가능
- **bedtools → pybedtools**: 완전 대체 가능
- **bedGraphToBigWig → pybigwig**: 완전 대체 가능

### 2. 부분 대체 가능한 도구들 (선택적 대체)
- **fastp → Biopython + cutadapt**: 기능은 대체 가능하지만 성능 고려 필요

### 3. 대체 어려운 도구들 (유지 권장)
- **BWA**: 매우 복잡한 알고리즘으로 Python 구현이 어려움

## 새로운 requirements.txt

```txt
# 기존 라이브러리들
pyspark>=3.0.0
pandas>=1.3.0
numpy>=1.21.0
matplotlib>=3.5.0
seaborn>=0.11.0

# 생물정보학 라이브러리들
pysam>=0.19.0
pybedtools>=0.8.0
pybigwig>=0.3.0
biopython>=1.79
cutadapt>=3.0

# 추가 유틸리티
pathlib2>=2.3.0
typing-extensions>=4.0.0
```

## 구현된 Python 도구 대체 파이프라인

`python_tools_replacement.py` 파일에 완전한 Python 라이브러리 기반 파이프라인이 구현되어 있습니다.

### 주요 특징:
1. **의존성 확인**: 필요한 Python 라이브러리들의 설치 여부 확인
2. **Biopython 기반 전처리**: FASTQ 파일 품질 필터링 및 어댑터 제거
3. **pysam 기반 SAM 처리**: SAM/BAM 파일 변환, 정렬, 통계 계산
4. **pybedtools 기반 커버리지 계산**: 게놈 커버리지 분석
5. **pybigwig 기반 BigWig 생성**: 커버리지 시각화 파일 생성

### 사용법:
```bash
python python_tools_replacement.py \
    --reads-dir /path/to/reads \
    --reference-genome /path/to/reference.fa \
    --reference-index /path/to/reference.fa.fai
```

## 성능 고려사항

### 1. 메모리 사용량
- Python 라이브러리들은 일반적으로 더 많은 메모리를 사용
- 대용량 데이터 처리 시 메모리 모니터링 필요

### 2. 처리 속도
- **pysam, pybedtools, pybigwig**: 원본 도구와 비슷한 성능
- **Biopython 기반 전처리**: fastp보다 느릴 수 있음
- **간단한 매핑**: BWA보다 훨씬 느림

### 3. 정확도
- **pysam, pybedtools, pybigwig**: 원본 도구와 동일한 정확도
- **Biopython 기반 전처리**: 구현에 따라 정확도 차이 가능
- **간단한 매핑**: BWA보다 정확도가 낮음

## 결론

### 권장사항:
1. **samtools, bedtools, bedGraphToBigWig**는 Python 라이브러리로 완전 대체 가능
2. **fastp**는 Biopython + cutadapt로 대체 가능하지만 성능 고려 필요
3. **BWA**는 현재로서는 Python 라이브러리로 대체하기 어려움

### 하이브리드 접근법:
- 성능이 중요한 부분 (BWA)은 외부 도구 유지
- 데이터 처리 부분 (samtools, bedtools)은 Python 라이브러리 사용
- 전처리 부분은 선택적으로 Python 라이브러리 사용

이러한 접근을 통해 외부 의존성을 줄이면서도 성능과 정확도를 유지할 수 있습니다.
