#!/usr/bin/env python3
"""
올바른 FASTQ 파일 분할 스크립트
R1, R2 파일을 각각 청크로 분할하여 병렬 처리 가능하게 만듦
"""

import argparse
import subprocess
import logging
from pathlib import Path
from typing import List, Tuple

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def split_fastq_file(input_file: Path, output_dir: Path, chunk_prefix: str, 
                    lines_per_chunk: int = 1000000) -> List[Path]:
    """
    단일 FASTQ 파일을 청크로 분할
    
    Args:
        input_file: 입력 FASTQ 파일
        output_dir: 출력 디렉토리
        chunk_prefix: 청크 파일 접두사
        lines_per_chunk: 청크당 라인 수
    
    Returns:
        생성된 청크 파일들의 경로 리스트
    """
    logger.info(f"파일 분할 시작: {input_file.name} -> {chunk_prefix}")
    
    # split 명령어 실행
    cmd = [
        "split",
        "-l", str(lines_per_chunk),
        "--numeric-suffixes=1",
        "--suffix-length=2",
        str(input_file),
        str(output_dir / f"{chunk_prefix}_chunk")
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"분할 완료: {input_file.name}")
        
        # 생성된 청크 파일들 찾기
        chunk_files = list(output_dir.glob(f"{chunk_prefix}_chunk*"))
        chunk_files.sort()  # 정렬
        
        logger.info(f"생성된 청크 파일 수: {len(chunk_files)}개")
        for chunk_file in chunk_files:
            logger.info(f"  - {chunk_file.name}")
        
        return chunk_files
        
    except subprocess.CalledProcessError as e:
        logger.error(f"파일 분할 실패: {e.stderr}")
        return []

def split_fastq_pairs(input_dir: Path, output_dir: Path, 
                     lines_per_chunk: int = 1000000) -> List[Tuple[Path, Path]]:
    """
    FASTQ 파일 쌍(R1, R2)을 각각 청크로 분할
    
    Args:
        input_dir: 입력 디렉토리 (R1, R2 파일이 있는 곳)
        output_dir: 출력 디렉토리
        lines_per_chunk: 청크당 라인 수
    
    Returns:
        (R1 청크, R2 청크) 튜플 리스트
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # R1, R2 파일 찾기
    r1_files = list(input_dir.glob("*_1.fastq"))
    r2_files = list(input_dir.glob("*_2.fastq"))
    
    if not r1_files or not r2_files:
        logger.error("R1 또는 R2 파일을 찾을 수 없습니다.")
        return []
    
    # 첫 번째 쌍만 처리 (여러 쌍이 있다면 첫 번째)
    r1_file = r1_files[0]
    r2_file = r2_files[0]
    
    logger.info(f"처리할 파일 쌍:")
    logger.info(f"  R1: {r1_file.name}")
    logger.info(f"  R2: {r2_file.name}")
    
    # 파일명에서 샘플 ID 추출
    sample_id = r1_file.stem.replace("_1", "")
    
    # R1 파일 분할
    r1_chunks = split_fastq_file(
        r1_file, 
        output_dir, 
        f"{sample_id}_1", 
        lines_per_chunk
    )
    
    # R2 파일 분할
    r2_chunks = split_fastq_file(
        r2_file, 
        output_dir, 
        f"{sample_id}_2", 
        lines_per_chunk
    )
    
    if len(r1_chunks) != len(r2_chunks):
        logger.error(f"R1과 R2 청크 수가 다릅니다: R1={len(r1_chunks)}, R2={len(r2_chunks)}")
        return []
    
    # 청크 쌍 생성
    chunk_pairs = list(zip(r1_chunks, r2_chunks))
    
    logger.info(f"총 {len(chunk_pairs)}개의 청크 쌍 생성 완료")
    for i, (r1_chunk, r2_chunk) in enumerate(chunk_pairs, 1):
        logger.info(f"  청크 {i}: {r1_chunk.name} <-> {r2_chunk.name}")
    
    return chunk_pairs

def main():
    parser = argparse.ArgumentParser(description="FASTQ 파일을 청크로 분할")
    parser.add_argument("--input-dir", type=Path, default=Path("data/reads"),
                       help="입력 디렉토리 (기본값: data/reads)")
    parser.add_argument("--output-dir", type=Path, default=Path("data/reads_chunks"),
                       help="출력 디렉토리 (기본값: data/reads_chunks)")
    parser.add_argument("--lines-per-chunk", type=int, default=1000000,
                       help="청크당 라인 수 (기본값: 1000000)")
    
    args = parser.parse_args()
    
    logger.info("FASTQ 파일 분할 시작")
    logger.info(f"입력 디렉토리: {args.input_dir}")
    logger.info(f"출력 디렉토리: {args.output_dir}")
    logger.info(f"청크당 라인 수: {args.lines_per_chunk}")
    
    # 기존 출력 디렉토리 정리
    if args.output_dir.exists():
        import shutil
        shutil.rmtree(args.output_dir)
        logger.info(f"기존 출력 디렉토리 삭제: {args.output_dir}")
    
    # 파일 분할
    chunk_pairs = split_fastq_pairs(args.input_dir, args.output_dir, args.lines_per_chunk)
    
    if chunk_pairs:
        logger.info("파일 분할 완료!")
        logger.info(f"생성된 청크 쌍 수: {len(chunk_pairs)}개")
        logger.info(f"출력 위치: {args.output_dir}")
    else:
        logger.error("파일 분할에 실패했습니다.")

if __name__ == "__main__":
    main()
