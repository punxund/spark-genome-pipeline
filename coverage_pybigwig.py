#!/usr/bin/env python3
"""
pyBigWig를 사용한 BigWig 파일 생성 모듈
"""

import pyBigWig
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def create_bigwig_from_bed(bed_file: Path, reference_index: Path, output_bigwig: Path) -> bool:
    """BED 파일을 읽어서 BigWig 파일을 생성합니다."""
    try:
        # 참조 게놈 크기 정보 읽기
        chrom_sizes = {}
        with open(reference_index, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    chrom_sizes[parts[0]] = int(parts[1])
        
        # BED 파일 읽기
        df = pd.read_csv(bed_file, sep='\t', header=None, 
                       names=['chrom', 'start', 'end', 'coverage'])
        
        # BigWig 파일 생성
        with pyBigWig.open(str(output_bigwig), 'w') as bw:
            bw.addHeader(list(chrom_sizes.items()))
            
            for _, row in df.iterrows():
                chrom = row['chrom']
                start = int(row['start'])
                end = int(row['end'])
                coverage = float(row['coverage'])
                
                if coverage > 0:
                    bw.addEntries([chrom], [start], [end], [coverage])
        
        logger.info(f"BigWig 파일 생성 완료: {output_bigwig}")
        return True
        
    except Exception as e:
        logger.error(f"BigWig 파일 생성 실패: {e}")
        return False
