#!/bin/bash
"""
Spark Genome Pipeline 의존성 설치 스크립트
GitHub에서 클론한 후 실행하세요.
"""

set -e

echo "=== Spark Genome Pipeline 의존성 설치 ==="

# 1. Python 패키지 설치
echo "1. Python 패키지 설치 중..."
pip install -r requirements.txt

# 2. 시스템 도구 설치
echo "2. 시스템 도구 설치 중..."

# Ubuntu/Debian
if command -v apt-get &> /dev/null; then
    echo "Ubuntu/Debian 시스템 감지됨"
    sudo apt-get update
    sudo apt-get install -y fastp bwa samtools bedtools wget
    
elif command -v yum &> /dev/null; then
    echo "CentOS/RHEL 시스템 감지됨"
    sudo yum install -y fastp bwa samtools bedtools wget
    
elif command -v brew &> /dev/null; then
    echo "macOS (Homebrew) 시스템 감지됨"
    brew install fastp bwa samtools bedtools wget
    
else
    echo "지원되지 않는 패키지 매니저입니다."
    echo "수동으로 설치해주세요: fastp, bwa, samtools, bedtools"
fi

# 3. bedGraphToBigWig 설치
echo "3. bedGraphToBigWig 설치 중..."
if command -v bedGraphToBigWig &> /dev/null; then
    echo "bedGraphToBigWig가 이미 설치되어 있습니다: $(which bedGraphToBigWig)"
else
    echo "bedGraphToBigWig를 다운로드하고 설치합니다..."
    wget https://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/bedGraphToBigWig
    chmod +x bedGraphToBigWig
    sudo cp bedGraphToBigWig /usr/local/bin/
    rm bedGraphToBigWig
    echo "bedGraphToBigWig 설치 완료"
fi

# 4. 설치 확인
echo "4. 설치 확인 중..."
TOOLS=("fastp" "bwa" "samtools" "bedtools" "bedGraphToBigWig")
MISSING_TOOLS=()

for tool in "${TOOLS[@]}"; do
    if command -v "$tool" &> /dev/null; then
        echo "✅ $tool: $(which $tool)"
    else
        echo "❌ $tool: 설치되지 않음"
        MISSING_TOOLS+=("$tool")
    fi
done

if [ ${#MISSING_TOOLS[@]} -eq 0 ]; then
    echo ""
    echo "🎉 모든 의존성이 성공적으로 설치되었습니다!"
    echo ""
    echo "다음 명령어로 파이프라인을 실행할 수 있습니다:"
    echo "python3 main.py"
else
    echo ""
    echo "⚠️ 다음 도구들이 설치되지 않았습니다: ${MISSING_TOOLS[*]}"
    echo "수동으로 설치해주세요."
    exit 1
fi
