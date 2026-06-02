#!/usr/bin/env python3
"""
리소스 사용량 비교 분석 스크립트 (향상된 버전)
프로세스별 리소스 데이터를 활용하여 의미 있는 시각화 생성
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse
import sys
import io

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

def analyze_process_resources(gencov_file, spark_file):
    """프로세스별 리소스 사용량 분석"""
    print("=== 프로세스별 리소스 사용량 분석 ===")
    
    # 데이터 로드
    gencov_df = load_process_data(gencov_file)
    spark_df = load_process_data(spark_file)
    
    if gencov_df is None or spark_df is None:
        print("프로세스 데이터 로드 실패")
        return None, None
    
    print(f"genCov 프로세스 데이터: {len(gencov_df)} 행")
    print(f"Spark 프로세스 데이터: {len(spark_df)} 행")
    
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
    
    return gencov_df, spark_df

def create_enhanced_visualization(gencov_df, spark_df, output_dir):
    """향상된 시각화 생성 (프로세스별 데이터 활용)"""
    print("\n=== 향상된 시각화 생성 중 ===")
    
    # 그래프 스타일 설정
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. 프로세스별 최대 CPU 사용률 비교
    processes = ['fastp', 'bwa', 'java', 'python', 'spark']
    gencov_cpu_max = []
    spark_cpu_max = []
    
    for proc in processes:
        gencov_proc = gencov_df[gencov_df['Process_Name'] == proc]
        spark_proc = spark_df[spark_df['Process_Name'] == proc]
        
        gencov_cpu = gencov_proc['CPU(%)'].dropna().max() if not gencov_proc.empty and 'CPU(%)' in gencov_proc.columns else 0
        spark_cpu = spark_proc['CPU(%)'].dropna().max() if not spark_proc.empty and 'CPU(%)' in spark_proc.columns else 0
        
        gencov_cpu_max.append(gencov_cpu if not pd.isna(gencov_cpu) else 0)
        spark_cpu_max.append(spark_cpu if not pd.isna(spark_cpu) else 0)
    
    x = np.arange(len(processes))
    width = 0.35
    
    bars1 = axes[0, 0].bar(x - width/2, gencov_cpu_max, width, label='genCov', color='#ff7f0e', alpha=0.7)
    bars2 = axes[0, 0].bar(x + width/2, spark_cpu_max, width, label='Spark', color='#1f77b4', alpha=0.7)
    
    axes[0, 0].set_title('프로세스별 최대 CPU 사용률 비교', fontsize=14, fontweight='bold')
    axes[0, 0].set_ylabel('CPU 사용률 (%)', fontsize=12)
    axes[0, 0].set_xlabel('프로세스', fontsize=12)
    axes[0, 0].set_xticks(x)
    axes[0, 0].set_xticklabels(processes, rotation=45)
    axes[0, 0].legend(fontsize=11)
    axes[0, 0].grid(True, alpha=0.3)
    
    # 값 표시
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                axes[0, 0].text(bar.get_x() + bar.get_width()/2., height + max(max(gencov_cpu_max), max(spark_cpu_max))*0.01,
                               f'{height:.1f}', ha='center', va='bottom', fontsize=9)
    
    # 2. 프로세스별 최대 메모리 사용량 비교
    gencov_mem_max = []
    spark_mem_max = []
    
    for proc in processes:
        gencov_proc = gencov_df[gencov_df['Process_Name'] == proc]
        spark_proc = spark_df[spark_df['Process_Name'] == proc]
        
        gencov_mem = gencov_proc['Memory(MB)'].dropna().max() if not gencov_proc.empty and 'Memory(MB)' in gencov_proc.columns else 0
        spark_mem = spark_proc['Memory(MB)'].dropna().max() if not spark_proc.empty and 'Memory(MB)' in spark_proc.columns else 0
        
        gencov_mem_max.append(gencov_mem if not pd.isna(gencov_mem) else 0)
        spark_mem_max.append(spark_mem if not pd.isna(spark_mem) else 0)
    
    bars1 = axes[0, 1].bar(x - width/2, gencov_mem_max, width, label='genCov', color='#ff7f0e', alpha=0.7)
    bars2 = axes[0, 1].bar(x + width/2, spark_mem_max, width, label='Spark', color='#1f77b4', alpha=0.7)
    
    axes[0, 1].set_title('프로세스별 최대 메모리 사용량 비교', fontsize=14, fontweight='bold')
    axes[0, 1].set_ylabel('메모리 사용량 (MB)', fontsize=12)
    axes[0, 1].set_xlabel('프로세스', fontsize=12)
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels(processes, rotation=45)
    axes[0, 1].legend(fontsize=11)
    axes[0, 1].grid(True, alpha=0.3)
    
    # 값 표시
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                axes[0, 1].text(bar.get_x() + bar.get_width()/2., height + max(max(gencov_mem_max), max(spark_mem_max))*0.01,
                               f'{height:.1f}', ha='center', va='bottom', fontsize=9)
    
    # 3. 프로세스별 평균 스레드 수 비교
    gencov_thread_avg = []
    spark_thread_avg = []
    
    for proc in processes:
        gencov_proc = gencov_df[gencov_df['Process_Name'] == proc]
        spark_proc = spark_df[spark_df['Process_Name'] == proc]
        
        gencov_thread = gencov_proc['Threads'].dropna().mean() if not gencov_proc.empty and 'Threads' in gencov_proc.columns else 0
        spark_thread = spark_proc['Threads'].dropna().mean() if not spark_proc.empty and 'Threads' in spark_proc.columns else 0
        
        gencov_thread_avg.append(gencov_thread if not pd.isna(gencov_thread) else 0)
        spark_thread_avg.append(spark_thread if not pd.isna(spark_thread) else 0)
    
    bars1 = axes[1, 0].bar(x - width/2, gencov_thread_avg, width, label='genCov', color='#ff7f0e', alpha=0.7)
    bars2 = axes[1, 0].bar(x + width/2, spark_thread_avg, width, label='Spark', color='#1f77b4', alpha=0.7)
    
    axes[1, 0].set_title('프로세스별 평균 스레드 수 비교', fontsize=14, fontweight='bold')
    axes[1, 0].set_ylabel('평균 스레드 수', fontsize=12)
    axes[1, 0].set_xlabel('프로세스', fontsize=12)
    axes[1, 0].set_xticks(x)
    axes[1, 0].set_xticklabels(processes, rotation=45)
    axes[1, 0].legend(fontsize=11)
    axes[1, 0].grid(True, alpha=0.3)
    
    # 값 표시
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                axes[1, 0].text(bar.get_x() + bar.get_width()/2., height + max(max(gencov_thread_avg), max(spark_thread_avg))*0.01,
                               f'{height:.1f}', ha='center', va='bottom', fontsize=9)
    
    # 4. 시간별 CPU 사용률 변화 (Spark만, 값이 있는 경우)
    if not spark_df.empty and 'CPU(%)' in spark_df.columns:
        # 시간별로 그룹화하여 평균 CPU 사용률 계산
        spark_time_cpu = spark_df.groupby('Timestamp')['CPU(%)'].mean().reset_index()
        spark_time_cpu = spark_time_cpu.dropna()
        
        if not spark_time_cpu.empty:
            axes[1, 1].plot(spark_time_cpu['Timestamp'], spark_time_cpu['CPU(%)'], 
                           color='#1f77b4', linewidth=2, marker='o', markersize=4)
            axes[1, 1].set_title('Spark 파이프라인 시간별 평균 CPU 사용률', fontsize=14, fontweight='bold')
            axes[1, 1].set_ylabel('평균 CPU 사용률 (%)', fontsize=12)
            axes[1, 1].set_xlabel('시간', fontsize=12)
            axes[1, 1].tick_params(axis='x', rotation=45)
            axes[1, 1].grid(True, alpha=0.3)
            
            # 최대값 표시
            max_cpu = spark_time_cpu['CPU(%)'].max()
            max_time = spark_time_cpu.loc[spark_time_cpu['CPU(%)'].idxmax(), 'Timestamp']
            axes[1, 1].annotate(f'최대: {max_cpu:.1f}%', 
                               xy=(max_time, max_cpu), xytext=(10, 10),
                               textcoords='offset points', ha='left', va='bottom',
                               bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                               arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
        else:
            axes[1, 1].text(0.5, 0.5, 'CPU 데이터 없음', ha='center', va='center', 
                           transform=axes[1, 1].transAxes, fontsize=14)
            axes[1, 1].set_title('Spark 파이프라인 시간별 평균 CPU 사용률', fontsize=14, fontweight='bold')
    else:
        axes[1, 1].text(0.5, 0.5, 'CPU 컬럼 없음', ha='center', va='center', 
                       transform=axes[1, 1].transAxes, fontsize=14)
        axes[1, 1].set_title('Spark 파이프라인 시간별 평균 CPU 사용률', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    
    # 출력 디렉토리 생성
    Path(output_dir).mkdir(exist_ok=True)
    
    # 그래프 저장
    output_file = f'{output_dir}/enhanced_resource_comparison.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"향상된 시각화 저장: {output_file}")
    
    # 그래프 표시
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='향상된 리소스 사용량 비교 분석')
    parser.add_argument('--gencov-process', required=True, help='genCov 프로세스별 리소스 로그 파일')
    parser.add_argument('--spark-process', required=True, help='Spark 프로세스별 리소스 로그 파일')
    parser.add_argument('--output-dir', default='resource_analysis', help='결과 저장 디렉토리')
    
    args = parser.parse_args()
    
    # 출력 디렉토리 생성
    Path(args.output_dir).mkdir(exist_ok=True)
    
    # 프로세스별 리소스 분석
    gencov_df, spark_df = analyze_process_resources(args.gencov_process, args.spark_process)
    
    # 향상된 시각화 생성
    if gencov_df is not None and spark_df is not None:
        create_enhanced_visualization(gencov_df, spark_df, args.output_dir)
    
    print(f"\n향상된 분석 완료! 결과 저장 위치: {args.output_dir}")

if __name__ == "__main__":
    main()

