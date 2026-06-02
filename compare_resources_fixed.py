#!/usr/bin/env python3
"""
리소스 사용량 비교 분석 스크립트 (수정된 버전)
genCov와 Spark Hybrid의 시스템 리소스 사용량을 비교
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import argparse
import sys
import io

def load_resource_data(file_path):
    """리소스 사용량 데이터 로드 (헤더 정보 건너뛰기)"""
    try:
        # 헤더 정보를 건너뛰고 실제 데이터만 로드
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # CSV 헤더를 찾기
        csv_start = None
        for i, line in enumerate(lines):
            if line.startswith('Timestamp,CPU_Usage'):
                csv_start = i
                break
        
        if csv_start is None:
            print(f"CSV 헤더를 찾을 수 없습니다: {file_path}")
            return None
        
        # CSV 데이터만 추출하여 DataFrame 생성
        csv_data = lines[csv_start:]
        df = pd.read_csv(io.StringIO(''.join(csv_data)))
        
        # 타임스탬프를 datetime으로 변환
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        # 빈 값 처리
        df = df.replace('', np.nan)
        
        # 숫자 컬럼들을 float로 변환
        numeric_columns = ['CPU_Usage(%)', 'Memory_Total(GB)', 'Memory_Used(GB)', 
                          'Memory_Free(GB)', 'Memory_Cached(GB)', 'Disk_IO_Read(MB/s)',
                          'Disk_IO_Write(MB/s)', 'Load_Avg_1min', 'Load_Avg_5min', 
                          'Load_Avg_15min', 'Process_Count']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        print(f"데이터 로드 실패: {e}")
        return None

def load_process_data(file_path):
    """프로세스별 리소스 데이터 로드 (헤더 정보 건너뛰기)"""
    try:
        # 헤더 정보를 건너뛰고 실제 데이터만 로드
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # CSV 헤더를 찾기
        csv_start = None
        for i, line in enumerate(lines):
            if line.startswith('Timestamp,Process_Name'):
                csv_start = i
                break
        
        if csv_start is None:
            print(f"프로세스 CSV 헤더를 찾을 수 없습니다: {file_path}")
            return None
        
        # CSV 데이터만 추출하여 DataFrame 생성
        csv_data = lines[csv_start:]
        df = pd.read_csv(io.StringIO(''.join(csv_data)))
        
        # 타임스탬프를 datetime으로 변환
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        # 빈 값 처리
        df = df.replace('', np.nan)
        
        # 숫자 컬럼들을 float로 변환
        numeric_columns = ['CPU(%)', 'Memory(MB)', 'Memory_Percent(%)', 'Threads']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        print(f"프로세스 데이터 로드 실패: {e}")
        return None

def analyze_system_resources(gencov_file, spark_file):
    """시스템 리소스 사용량 분석"""
    print("=== 시스템 리소스 사용량 분석 ===")
    
    # 데이터 로드
    gencov_df = load_resource_data(gencov_file)
    spark_df = load_resource_data(spark_file)
    
    if gencov_df is None or spark_df is None:
        print("데이터 로드 실패")
        return None, None
    
    print(f"genCov 데이터: {len(gencov_df)} 행")
    print(f"Spark 데이터: {len(spark_df)} 행")
    
    # CPU 사용률 분석 (값이 있는 경우만)
    if 'CPU_Usage(%)' in gencov_df.columns and 'CPU_Usage(%)' in spark_df.columns:
        gencov_cpu = gencov_df['CPU_Usage(%)'].dropna()
        spark_cpu = spark_df['CPU_Usage(%)'].dropna()
        
        if not gencov_cpu.empty and not spark_cpu.empty:
            print("\n1. CPU 사용률 비교:")
            print(f"genCov - 최대: {gencov_cpu.max():.1f}%, 평균: {gencov_cpu.mean():.1f}%")
            print(f"Spark  - 최대: {spark_cpu.max():.1f}%, 평균: {spark_cpu.mean():.1f}%")
    
    # 메모리 사용량 분석
    if 'Memory_Used(GB)' in gencov_df.columns and 'Memory_Used(GB)' in spark_df.columns:
        gencov_mem = gencov_df['Memory_Used(GB)'].dropna()
        spark_mem = spark_df['Memory_Used(GB)'].dropna()
        
        if not gencov_mem.empty and not spark_mem.empty:
            print("\n2. 메모리 사용량 비교:")
            print(f"genCov - 최대: {gencov_mem.max():.1f}GB, 평균: {gencov_mem.mean():.1f}GB")
            print(f"Spark  - 최대: {spark_mem.max():.1f}GB, 평균: {spark_mem.mean():.1f}GB")
    
    # 디스크 I/O 분석
    if 'Disk_IO_Read(MB/s)' in gencov_df.columns and 'Disk_IO_Write(MB/s)' in gencov_df.columns:
        gencov_read = gencov_df['Disk_IO_Read(MB/s)'].dropna()
        gencov_write = gencov_df['Disk_IO_Write(MB/s)'].dropna()
        spark_read = spark_df['Disk_IO_Read(MB/s)'].dropna()
        spark_write = spark_df['Disk_IO_Write(MB/s)'].dropna()
        
        if not gencov_read.empty and not spark_read.empty:
            print("\n3. 디스크 I/O 비교:")
            print(f"genCov - 읽기 최대: {gencov_read.max():.1f}MB/s, 쓰기 최대: {gencov_write.max():.1f}MB/s")
            print(f"Spark  - 읽기 최대: {spark_read.max():.1f}MB/s, 쓰기 최대: {spark_write.max():.1f}MB/s")
    
    # 시스템 로드 분석
    if 'Load_Avg_1min' in gencov_df.columns and 'Load_Avg_1min' in spark_df.columns:
        gencov_load = gencov_df['Load_Avg_1min'].dropna()
        spark_load = spark_df['Load_Avg_1min'].dropna()
        
        if not gencov_load.empty and not spark_load.empty:
            print("\n4. 시스템 로드 비교:")
            print(f"genCov - 1분 평균 최대: {gencov_load.max():.2f}")
            print(f"Spark  - 1분 평균 최대: {spark_load.max():.2f}")
    
    return gencov_df, spark_df

def analyze_process_resources(gencov_file, spark_file):
    """프로세스별 리소스 사용량 분석"""
    print("\n=== 프로세스별 리소스 사용량 분석 ===")
    
    # 데이터 로드
    gencov_df = load_process_data(gencov_file)
    spark_df = load_process_data(spark_file)
    
    if gencov_df is None or spark_df is None:
        print("프로세스 데이터 로드 실패")
        return
    
    # 프로세스별 분석
    processes = ['fastp', 'bwa', 'java', 'python', 'spark']
    
    for proc in processes:
        gencov_proc = gencov_df[gencov_df['Process_Name'] == proc]
        spark_proc = spark_df[spark_df['Process_Name'] == proc]
        
        if not gencov_proc.empty:
            print(f"\n{proc} (genCov):")
            if 'CPU(%)' in gencov_proc.columns:
                cpu_data = gencov_proc['CPU(%)'].dropna()
                if not cpu_data.empty:
                    print(f"  최대 CPU: {cpu_data.max():.1f}%")
            if 'Memory(MB)' in gencov_proc.columns:
                mem_data = gencov_proc['Memory(MB)'].dropna()
                if not mem_data.empty:
                    print(f"  최대 메모리: {mem_data.max():.1f}MB")
            if 'Threads' in gencov_proc.columns:
                thread_data = gencov_proc['Threads'].dropna()
                if not thread_data.empty:
                    print(f"  평균 스레드: {thread_data.mean():.1f}")
        
        if not spark_proc.empty:
            print(f"{proc} (Spark):")
            if 'CPU(%)' in spark_proc.columns:
                cpu_data = spark_proc['CPU(%)'].dropna()
                if not cpu_data.empty:
                    print(f"  최대 CPU: {cpu_data.max():.1f}%")
            if 'Memory(MB)' in spark_proc.columns:
                mem_data = spark_proc['Memory(MB)'].dropna()
                if not mem_data.empty:
                    print(f"  최대 메모리: {mem_data.max():.1f}MB")
            if 'Threads' in spark_proc.columns:
                thread_data = spark_proc['Threads'].dropna()
                if not thread_data.empty:
                    print(f"  평균 스레드: {thread_data.mean():.1f}")

def create_visualization(gencov_df, spark_df, output_dir):
    """시각화 생성"""
    print("\n=== 시각화 생성 중 ===")
    
    # 그래프 스타일 설정
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. CPU 사용률 시간별 변화 (값이 있는 경우만)
    if 'CPU_Usage(%)' in gencov_df.columns and 'CPU_Usage(%)' in spark_df.columns:
        gencov_cpu = gencov_df[['Timestamp', 'CPU_Usage(%)']].dropna()
        spark_cpu = spark_df[['Timestamp', 'CPU_Usage(%)']].dropna()
        
        if not gencov_cpu.empty and not spark_cpu.empty:
            axes[0, 0].plot(gencov_cpu['Timestamp'], gencov_cpu['CPU_Usage(%)'], 
                           label='genCov', alpha=0.7, linewidth=2)
            axes[0, 0].plot(spark_cpu['Timestamp'], spark_cpu['CPU_Usage(%)'], 
                           label='Spark', alpha=0.7, linewidth=2)
            axes[0, 0].set_title('CPU 사용률 시간별 변화', fontsize=14, fontweight='bold')
            axes[0, 0].set_ylabel('CPU 사용률 (%)', fontsize=12)
            axes[0, 0].legend(fontsize=11)
            axes[0, 0].tick_params(axis='x', rotation=45)
            axes[0, 0].grid(True, alpha=0.3)
        else:
            axes[0, 0].text(0.5, 0.5, 'CPU 데이터 없음', ha='center', va='center', 
                           transform=axes[0, 0].transAxes, fontsize=14)
            axes[0, 0].set_title('CPU 사용률 시간별 변화', fontsize=14, fontweight='bold')
    else:
        axes[0, 0].text(0.5, 0.5, 'CPU 컬럼 없음', ha='center', va='center', 
                       transform=axes[0, 0].transAxes, fontsize=14)
        axes[0, 0].set_title('CPU 사용률 시간별 변화', fontsize=14, fontweight='bold')
    
    # 2. 메모리 사용량 시간별 변화
    if 'Memory_Used(GB)' in gencov_df.columns and 'Memory_Used(GB)' in spark_df.columns:
        gencov_mem = gencov_df[['Timestamp', 'Memory_Used(GB)']].dropna()
        spark_mem = spark_df[['Timestamp', 'Memory_Used(GB)']].dropna()
        
        if not gencov_mem.empty and not spark_mem.empty:
            axes[0, 1].plot(gencov_mem['Timestamp'], gencov_mem['Memory_Used(GB)'], 
                           label='genCov', alpha=0.7, linewidth=2)
            axes[0, 1].plot(spark_mem['Timestamp'], spark_mem['Memory_Used(GB)'], 
                           label='Spark', alpha=0.7, linewidth=2)
            axes[0, 1].set_title('메모리 사용량 시간별 변화', fontsize=14, fontweight='bold')
            axes[0, 1].set_ylabel('메모리 사용량 (GB)', fontsize=12)
            axes[0, 1].legend(fontsize=11)
            axes[0, 1].tick_params(axis='x', rotation=45)
            axes[0, 1].grid(True, alpha=0.3)
        else:
            axes[0, 1].text(0.5, 0.5, '메모리 데이터 없음', ha='center', va='center', 
                           transform=axes[0, 1].transAxes, fontsize=14)
            axes[0, 1].set_title('메모리 사용량 시간별 변화', fontsize=14, fontweight='bold')
    else:
        axes[0, 1].text(0.5, 0.5, '메모리 컬럼 없음', ha='center', va='center', 
                       transform=axes[0, 1].transAxes, fontsize=14)
        axes[0, 1].set_title('메모리 사용량 시간별 변화', fontsize=14, fontweight='bold')
    
    # 3. 디스크 I/O 비교
    if 'Disk_IO_Read(MB/s)' in gencov_df.columns and 'Disk_IO_Write(MB/s)' in gencov_df.columns:
        gencov_read = gencov_df['Disk_IO_Read(MB/s)'].dropna().max()
        gencov_write = gencov_df['Disk_IO_Write(MB/s)'].dropna().max()
        spark_read = spark_df['Disk_IO_Read(MB/s)'].dropna().max()
        spark_write = spark_df['Disk_IO_Write(MB/s)'].dropna().max()
        
        if not (pd.isna(gencov_read) or pd.isna(gencov_write) or pd.isna(spark_read) or pd.isna(spark_write)):
            labels = ['genCov 읽기', 'genCov 쓰기', 'Spark 읽기', 'Spark 쓰기']
            values = [gencov_read, gencov_write, spark_read, spark_write]
            colors = ['#ff7f0e', '#ff7f0e', '#1f77b4', '#1f77b4']
            
            bars = axes[1, 0].bar(labels, values, color=colors, alpha=0.7)
            axes[1, 0].set_title('최대 디스크 I/O 비교', fontsize=14, fontweight='bold')
            axes[1, 0].set_ylabel('MB/s', fontsize=12)
            axes[1, 0].grid(True, alpha=0.3)
            
            # 값 표시
            for bar, value in zip(bars, values):
                height = bar.get_height()
                axes[1, 0].text(bar.get_x() + bar.get_width()/2., height + max(values)*0.01,
                               f'{value:.1f}', ha='center', va='bottom', fontsize=10)
        else:
            axes[1, 0].text(0.5, 0.5, '디스크 I/O 데이터 없음', ha='center', va='center', 
                           transform=axes[1, 0].transAxes, fontsize=14)
            axes[1, 0].set_title('최대 디스크 I/O 비교', fontsize=14, fontweight='bold')
    else:
        axes[1, 0].text(0.5, 0.5, '디스크 I/O 컬럼 없음', ha='center', va='center', 
                       transform=axes[1, 0].transAxes, fontsize=14)
        axes[1, 0].set_title('최대 디스크 I/O 비교', fontsize=14, fontweight='bold')
    
    # 4. 시스템 로드 비교
    if 'Load_Avg_1min' in gencov_df.columns and 'Load_Avg_1min' in spark_df.columns:
        gencov_load = gencov_df['Load_Avg_1min'].dropna().max()
        spark_load = spark_df['Load_Avg_1min'].dropna().max()
        
        if not (pd.isna(gencov_load) or pd.isna(spark_load)):
            labels = ['genCov 1분', 'Spark 1분']
            values = [gencov_load, spark_load]
            colors = ['#ff7f0e', '#1f77b4']
            
            bars = axes[1, 1].bar(labels, values, color=colors, alpha=0.7)
            axes[1, 1].set_title('최대 시스템 로드 비교', fontsize=14, fontweight='bold')
            axes[1, 1].set_ylabel('Load Average', fontsize=12)
            axes[1, 1].grid(True, alpha=0.3)
            
            # 값 표시
            for bar, value in zip(bars, values):
                height = bar.get_height()
                axes[1, 1].text(bar.get_x() + bar.get_width()/2., height + max(values)*0.01,
                               f'{value:.2f}', ha='center', va='bottom', fontsize=10)
        else:
            axes[1, 1].text(0.5, 0.5, '시스템 로드 데이터 없음', ha='center', va='center', 
                           transform=axes[1, 1].transAxes, fontsize=14)
            axes[1, 1].set_title('최대 시스템 로드 비교', fontsize=14, fontweight='bold')
    else:
        axes[1, 1].text(0.5, 0.5, '시스템 로드 컬럼 없음', ha='center', va='center', 
                       transform=axes[1, 1].transAxes, fontsize=14)
        axes[1, 1].set_title('최대 시스템 로드 비교', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    
    # 출력 디렉토리 생성
    Path(output_dir).mkdir(exist_ok=True)
    
    # 그래프 저장
    output_file = f'{output_dir}/resource_comparison.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"시각화 저장: {output_file}")
    
    # 그래프 표시
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='리소스 사용량 비교 분석 (수정된 버전)')
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
