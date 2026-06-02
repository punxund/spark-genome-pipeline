#!/usr/bin/env python3
"""
리소스 사용량 비교 분석 스크립트
genCov와 Spark Hybrid의 시스템 리소스 사용량을 비교
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import argparse
import sys

def load_resource_data(file_path):
    """리소스 사용량 데이터 로드"""
    try:
        df = pd.read_csv(file_path)
        # 타임스탬프를 datetime으로 변환
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        return df
    except Exception as e:
        print(f"데이터 로드 실패: {e}")
        return None

def analyze_system_resources(gencov_file, spark_file):
    """시스템 리소스 사용량 분석"""
    print("=== 시스템 리소스 사용량 분석 ===")
    
    # 데이터 로드
    gencov_df = load_resource_data(gencov_file)
    spark_df = load_resource_data(spark_file)
    
    if gencov_df is None or spark_df is None:
        print("데이터 로드 실패")
        return
    
    # CPU 사용률 분석
    print("\n1. CPU 사용률 비교:")
    print(f"genCov - 최대: {gencov_df['CPU_Usage(%)'].max():.1f}%, 평균: {gencov_df['CPU_Usage(%)'].mean():.1f}%")
    print(f"Spark  - 최대: {spark_df['CPU_Usage(%)'].max():.1f}%, 평균: {spark_df['CPU_Usage(%)'].mean():.1f}%")
    
    # 메모리 사용량 분석
    print("\n2. 메모리 사용량 비교:")
    print(f"genCov - 최대: {gencov_df['Memory_Used(GB)'].max():.1f}GB, 평균: {gencov_df['Memory_Used(GB)'].mean():.1f}GB")
    print(f"Spark  - 최대: {spark_df['Memory_Used(GB)'].max():.1f}GB, 평균: {spark_df['Memory_Used(GB)'].mean():.1f}GB")
    
    # 디스크 I/O 분석
    print("\n3. 디스크 I/O 비교:")
    print(f"genCov - 읽기 최대: {gencov_df['Disk_IO_Read(MB/s)'].max():.1f}MB/s, 쓰기 최대: {gencov_df['Disk_IO_Write(MB/s)'].max():.1f}MB/s")
    print(f"Spark  - 읽기 최대: {spark_df['Disk_IO_Read(MB/s)'].max():.1f}MB/s, 쓰기 최대: {spark_df['Disk_IO_Write(MB/s)'].max():.1f}MB/s")
    
    # 시스템 로드 분석
    print("\n4. 시스템 로드 비교:")
    print(f"genCov - 1분 평균 최대: {gencov_df['Load_Avg_1min'].max():.2f}")
    print(f"Spark  - 1분 평균 최대: {spark_df['Load_Avg_1min'].max():.2f}")
    
    return gencov_df, spark_df

def analyze_process_resources(gencov_file, spark_file):
    """프로세스별 리소스 사용량 분석"""
    print("\n=== 프로세스별 리소스 사용량 분석 ===")
    
    # 데이터 로드
    gencov_df = load_resource_data(gencov_file)
    spark_df = load_resource_data(spark_file)
    
    if gencov_df is None or spark_df is None:
        print("데이터 로드 실패")
        return
    
    # 프로세스별 분석
    processes = ['fastp', 'bwa', 'java', 'python']
    
    for proc in processes:
        gencov_proc = gencov_df[gencov_df['Process_Name'] == proc]
        spark_proc = spark_df[spark_df['Process_Name'] == proc]
        
        if not gencov_proc.empty:
            print(f"\n{proc} (genCov):")
            print(f"  최대 CPU: {gencov_proc['CPU(%)'].max():.1f}%")
            print(f"  최대 메모리: {gencov_proc['Memory(MB)'].max():.1f}MB")
            print(f"  평균 스레드: {gencov_proc['Threads'].mean():.1f}")
        
        if not spark_proc.empty:
            print(f"{proc} (Spark):")
            print(f"  최대 CPU: {spark_proc['CPU(%)'].max():.1f}%")
            print(f"  최대 메모리: {spark_proc['Memory(MB)'].max():.1f}MB")
            print(f"  평균 스레드: {spark_proc['Threads'].mean():.1f}")

def create_visualization(gencov_df, spark_df, output_dir):
    """시각화 생성"""
    print("\n=== 시각화 생성 중 ===")
    
    # 그래프 스타일 설정
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. CPU 사용률 시간별 변화
    axes[0, 0].plot(gencov_df['Timestamp'], gencov_df['CPU_Usage(%)'], label='genCov', alpha=0.7)
    axes[0, 0].plot(spark_df['Timestamp'], spark_df['CPU_Usage(%)'], label='Spark', alpha=0.7)
    axes[0, 0].set_title('CPU 사용률 시간별 변화')
    axes[0, 0].set_ylabel('CPU 사용률 (%)')
    axes[0, 0].legend()
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # 2. 메모리 사용량 시간별 변화
    axes[0, 1].plot(gencov_df['Timestamp'], gencov_df['Memory_Used(GB)'], label='genCov', alpha=0.7)
    axes[0, 1].plot(spark_df['Timestamp'], spark_df['Memory_Used(GB)'], label='Spark', alpha=0.7)
    axes[0, 1].set_title('메모리 사용량 시간별 변화')
    axes[0, 1].set_ylabel('메모리 사용량 (GB)')
    axes[0, 1].legend()
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # 3. 디스크 I/O 비교
    axes[1, 0].bar(['genCov 읽기', 'genCov 쓰기', 'Spark 읽기', 'Spark 쓰기'], 
                   [gencov_df['Disk_IO_Read(MB/s)'].max(), gencov_df['Disk_IO_Write(MB/s)'].max(),
                    spark_df['Disk_IO_Read(MB/s)'].max(), spark_df['Disk_IO_Write(MB/s)'].max()])
    axes[1, 0].set_title('최대 디스크 I/O 비교')
    axes[1, 0].set_ylabel('MB/s')
    
    # 4. 시스템 로드 비교
    axes[1, 1].bar(['genCov 1분', 'genCov 5분', 'genCov 15분', 'Spark 1분', 'Spark 5분', 'Spark 15분'],
                   [gencov_df['Load_Avg_1min'].max(), gencov_df['Load_Avg_5min'].max(), gencov_df['Load_Avg_15min'].max(),
                    spark_df['Load_Avg_1min'].max(), spark_df['Load_Avg_5min'].max(), spark_df['Load_Avg_15min'].max()])
    axes[1, 1].set_title('최대 시스템 로드 비교')
    axes[1, 1].set_ylabel('Load Average')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/resource_comparison.png', dpi=300, bbox_inches='tight')
    print(f"시각화 저장: {output_dir}/resource_comparison.png")

def main():
    parser = argparse.ArgumentParser(description='리소스 사용량 비교 분석')
    parser.add_argument('--gencov-system', required=True, help='genCov 시스템 리소스 로그 파일')
    parser.add_argument('--spark-system', required=True, help='Spark 시스템 리소스 로그 파일')
    parser.add_argument('--gencov-process', help='genCov 프로세스별 리소스 로그 파일')
    parser.add_argument('--spark-process', help='Spark 프로세스별 리소스 로그 파일')
    parser.add_argument('--output-dir', default='resource_analysis', help='결과 저장 디렉토리')
    
    args = parser.parse_args()
    
    # 출력 디렉토리 생성
    Path(args.output_dir).mkdir(exist_ok=True)
    
    # 시스템 리소스 분석
    gencov_df, spark_df = analyze_system_resources(args.gencov_system, args.spark_system)
    
    # 프로세스별 리소스 분석
    if args.gencov_process and args.spark_process:
        analyze_process_resources(args.gencov_process, args.spark_process)
    
    # 시각화 생성
    if gencov_df is not None and spark_df is not None:
        create_visualization(gencov_df, spark_df, args.output_dir)
    
    print(f"\n분석 완료! 결과 저장 위치: {args.output_dir}")

if __name__ == "__main__":
    main()



