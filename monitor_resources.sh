#!/bin/bash

# 시스템 리소스 실시간 모니터링 스크립트
# CPU, 메모리, 디스크 I/O, 네트워크 사용량 추적

OUTPUT_FILE="resource_usage_$(date +%Y%m%d_%H%M%S).log"
INTERVAL=5  # 5초마다 측정

echo "시스템 리소스 모니터링 시작: $(date)" | tee $OUTPUT_FILE
echo "측정 간격: ${INTERVAL}초" | tee -a $OUTPUT_FILE
echo "==========================================" | tee -a $OUTPUT_FILE

# 헤더 추가
echo "Timestamp,CPU_Usage(%),Memory_Total(GB),Memory_Used(GB),Memory_Free(GB),Memory_Cached(GB),Disk_IO_Read(MB/s),Disk_IO_Write(MB/s),Load_Avg_1min,Load_Avg_5min,Load_Avg_15min,Process_Count" | tee -a $OUTPUT_FILE

monitor_resources() {
    while true; do
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        
        # CPU 사용률 (전체)
        CPU_USAGE=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
        
        # 메모리 정보
        MEMORY_INFO=$(free -g | grep "Mem:")
        MEMORY_TOTAL=$(echo $MEMORY_INFO | awk '{print $2}')
        MEMORY_USED=$(echo $MEMORY_INFO | awk '{print $3}')
        MEMORY_FREE=$(echo $MEMORY_INFO | awk '{print $4}')
        MEMORY_CACHED=$(echo $MEMORY_INFO | awk '{print $6}')
        
        # 디스크 I/O (간단한 방법)
        DISK_IO=$(iostat -d 1 1 | tail -n +4 | head -n 1 | awk '{print $3, $4}')
        DISK_READ=$(echo $DISK_IO | awk '{print $1}')
        DISK_WRITE=$(echo $DISK_IO | awk '{print $2}')
        
        # 시스템 로드
        LOAD_AVG=$(uptime | awk -F'load average:' '{print $2}' | tr -d ' ')
        LOAD_1=$(echo $LOAD_AVG | cut -d',' -f1)
        LOAD_5=$(echo $LOAD_AVG | cut -d',' -f2)
        LOAD_15=$(echo $LOAD_AVG | cut -d',' -f3)
        
        # 프로세스 수
        PROCESS_COUNT=$(ps aux | wc -l)
        
        # 결과 출력
        echo "$TIMESTAMP,$CPU_USAGE,$MEMORY_TOTAL,$MEMORY_USED,$MEMORY_FREE,$MEMORY_CACHED,$DISK_READ,$DISK_WRITE,$LOAD_1,$LOAD_5,$LOAD_15,$PROCESS_COUNT" | tee -a $OUTPUT_FILE
        
        sleep $INTERVAL
    done
}

# Ctrl+C로 종료할 수 있도록 설정
trap 'echo "모니터링 종료: $(date)" | tee -a $OUTPUT_FILE; exit' INT

# 모니터링 시작
monitor_resources



