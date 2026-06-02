#!/usr/bin/env python3
"""
FASTQ 파일을 청크로 분할하는 스크립트
"""

import argparse
import subprocess
from pathlib import Path

def split_fastq_file(input_file: str, output_prefix: str, lines_per_chunk: int = 1000000):
    """FASTQ 파일을 지정된 라인 수로 분할합니다."""
    
    # split 명령어 실행
    cmd = f"split -l {lines_per_chunk} {input_file} {output_prefix}"
    
    print(f"실행 중: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✓ {input_file} 분할 완료")
        return True
    else:
        print(f"✗ {input_file} 분할 실패: {result.stderr}")
        return False

def main():
    parser = argparse.ArgumentParser(description="FASTQ 파일을 청크로 분할")
    parser.add_argument("--input-dir", required=True, help="입력 FASTQ 파일 디렉토리")
    parser.add_argument("--output-dir", required=True, help="출력 청크 파일 디렉토리")
    parser.add_argument("--chunks", type=int, default=6, help="분할할 청크 수")
    parser.add_argument("--lines-per-chunk", type=int, default=1000000, help="각 청크당 라인 수")
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # FASTQ 파일 쌍 찾기
    fastq_files = list(input_dir.glob("*.fastq"))
    fastq_pairs = []
    
    for f in fastq_files:
        if "_1.fastq" in f.name:
            r2_file = f.parent / f.name.replace("_1.fastq", "_2.fastq")
            if r2_file.exists():
                fastq_pairs.append((f, r2_file))
    
    print(f"발견된 FASTQ 쌍: {len(fastq_pairs)}개")
    
    for r1_file, r2_file in fastq_pairs:
        sample_name = r1_file.stem.replace("_1", "")
        
        print(f"\n처리 중: {sample_name}")
        
        # R1 파일 분할
        r1_output_prefix = output_dir / f"{sample_name}_chunk1_"
        success1 = split_fastq_file(str(r1_file), str(r1_output_prefix), args.lines_per_chunk)
        
        # R2 파일 분할
        r2_output_prefix = output_dir / f"{sample_name}_chunk2_"
        success2 = split_fastq_file(str(r2_file), str(r2_output_prefix), args.lines_per_chunk)
        
        if success1 and success2:
            print(f"✓ {sample_name} 분할 완료")
        else:
            print(f"✗ {sample_name} 분할 실패")

if __name__ == "__main__":
    main()






