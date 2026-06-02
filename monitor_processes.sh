#!/bin/bash

# 특정 프로세스들의 리소스 사용량 모니터링
# fastp, bwa, samtools, java, python 프로세스 추적

OUTPUT_FILE="process_usage_$(date +%Y%m%d_%H%M%S).log"
INTERVAL=2  # 2초마다 측정

echo "프로세스별 리소스 모니터링 시작: $(date)" | tee $OUTPUT_FILE
echo "측정 간격: ${INTERVAL}초" | tee -a $OUTPUT_FILE
echo "==========================================" | tee -a $OUTPUT_FILE

# 헤더 추가
echo "Timestamp,Process_Name,PID,CPU(%),Memory(MB),Memory_Percent(%),Threads,State,Command" | tee -a $OUTPUT_FILE

monitor_processes() {
    while true; do
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        
        # 관심 있는 프로세스들 모니터링
        PROCESSES=("fastp" "bwa" "samtools" "java" "python" "spark" "nextflow")
        
        for proc in "${PROCESSES[@]}"; do
            # ps 명령어로 프로세스 정보 수집
            ps_output=$(ps aux | grep -v grep | grep "$proc" | head -5)
            
            if [ ! -z "$ps_output" ]; then
                echo "$ps_output" | while read line; do
                    if [ ! -z "$line" ]; then
                        USER=$(echo $line | awk '{print $1}')
                        PID=$(echo $line | awk '{print $2}')
                        CPU=$(echo $line | awk '{print $3}')
                        MEM=$(echo $line | awk '{print $4}')
                        VSZ=$(echo $line | awk '{print $5}')
                        RSS=$(echo $line | awk '{print $6}')
                        TTY=$(echo $line | awk '{print $7}')
                        STAT=$(echo $line | awk '{print $8}')
                        START=$(echo $line | awk '{print $9}')
                        TIME=$(echo $line | awk '{print $10}')
                        COMMAND=$(echo $line | awk '{for(i=11;i<=NF;i++) printf $i" "; print ""}')
                        
                        # 스레드 수 계산
                        THREADS=$(ps -o thcount -p $PID 2>/dev/null | tail -n +2 | tr -d ' ')
                        if [ -z "$THREADS" ]; then THREADS="N/A"; fi
                        
                        # 메모리 사용량을 MB로 변환
                        MEM_MB=$(echo "scale=2; $RSS / 1024" | bc 2>/dev/null)
                        if [ -z "$MEM_MB" ]; then MEM_MB="N/A"; fi
                        
                        echo "$TIMESTAMP,$proc,$PID,$CPU,$MEM_MB,$MEM,$THREADS,$STAT,$COMMAND" | tee -a $OUTPUT_FILE
                    fi
                done
            fi
        done
        
        sleep $INTERVAL
    done
}

# Ctrl+C로 종료할 수 있도록 설정
trap 'echo "프로세스 모니터링 종료: $(date)" | tee -a $OUTPUT_FILE; exit' INT

# 모니터링 시작
monitor_processes



