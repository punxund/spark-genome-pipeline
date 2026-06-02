#!/bin/bash

# 로컬 데이터를 VM으로 업로드하는 스크립트

echo "=== 로컬 데이터를 VM으로 업로드 ==="

# hongsik1에 데이터 디렉토리 생성
ssh -o StrictHostKeyChecking=no kimhongs@hongsik1.vm.informatik.hu-berlin.de 'mkdir -p ~/spark-genome-pipeline/data'

# 참조 시퀀스 파일 업로드
echo "참조 시퀀스 파일 업로드 중..."
scp -o StrictHostKeyChecking=no data/ref_sequence_genB.fa* kimhongs@hongsik1.vm.informatik.hu-berlin.de:~/spark-genome-pipeline/data/

# FASTQ 파일 업로드
echo "FASTQ 파일 업로드 중..."
scp -o StrictHostKeyChecking=no data/reads/SRR30977596_1.fastq kimhongs@hongsik1.vm.informatik.hu-berlin.de:~/spark-genome-pipeline/data/
scp -o StrictHostKeyChecking=no data/reads/SRR30977596_2.fastq kimhongs@hongsik1.vm.informatik.hu-berlin.de:~/spark-genome-pipeline/data/

echo "데이터 업로드 완료!"



