# 샘플 데이터

이 폴더에는 파이프라인 테스트를 위한 작은 샘플 데이터가 포함되어 있습니다.

## 파일 설명

- `sample_1.fastq`: 샘플 R1 FASTQ 파일 (작은 크기)
- `sample_2.fastq`: 샘플 R2 FASTQ 파일 (작은 크기)
- `sample_reference.fa`: 샘플 참조 게놈 (작은 크기)

## 사용 방법

1. 샘플 데이터를 실제 데이터 폴더로 복사:
   ```bash
   mkdir -p ../data/reads
   cp sample_data/sample_1.fastq ../data/reads/
   cp sample_data/sample_2.fastq ../data/reads/
   cp sample_data/sample_reference.fa ../data/ref_sequence_genB.fa
   ```

2. 파이프라인 실행:
   ```bash
   python3 main.py
   ```

## 주의사항

- 이 샘플 데이터는 테스트 목적으로만 사용하세요
- 실제 분석에는 더 큰 데이터셋을 사용하세요
- 샘플 데이터는 교육 및 테스트 목적으로만 제공됩니다
