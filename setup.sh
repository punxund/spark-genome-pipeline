#!/bin/bash

# Spark Genome Pipeline 자동 설정 스크립트
# Git에서 클론한 후 실행하세요

set -e  # 오류 발생시 스크립트 중단

echo "=========================================="
echo "Spark Genome Pipeline 자동 설정"
echo "=========================================="

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 함수 정의
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 1. Python 버전 확인
print_info "Python 버전 확인 중..."
python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
required_version="3.8.0"

if python3 -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)"; then
    print_info "Python 버전 확인 완료: $python_version"
else
    print_error "Python 3.8+ 필요 (현재: $python_version)"
    exit 1
fi

# 2. Java 확인
print_info "Java 설치 확인 중..."
if command -v java &> /dev/null; then
    java_version=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    print_info "Java 설치됨: $java_version"
else
    print_error "Java가 설치되지 않았습니다."
    print_info "Java 설치 방법:"
    echo "  Ubuntu/Debian: sudo apt-get install openjdk-8-jdk"
    echo "  CentOS/RHEL: sudo yum install java-1.8.0-openjdk"
    exit 1
fi

# 3. 가상환경 생성
print_info "Python 가상환경 생성 중..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_info "가상환경 생성 완료"
else
    print_info "가상환경이 이미 존재합니다"
fi

# 4. 가상환경 활성화
print_info "가상환경 활성화 중..."
source venv/bin/activate

# 5. Python 패키지 설치
print_info "Python 패키지 설치 중..."
pip install --upgrade pip
pip install -r requirements.txt
print_info "Python 패키지 설치 완료"

# 6. 생물정보학 도구 설치
print_info "생물정보학 도구 설치 중..."
if [ -f "install_dependencies.sh" ]; then
    chmod +x install_dependencies.sh
    ./install_dependencies.sh
else
    print_warning "install_dependencies.sh 파일이 없습니다. 수동으로 도구를 설치하세요."
fi

# 7. 필요한 디렉토리 생성
print_info "필요한 디렉토리 생성 중..."
mkdir -p data/reads
mkdir -p results/spark_pipeline
mkdir -p temp
print_info "디렉토리 생성 완료"

# 8. 환경 검사 실행
print_info "환경 검사 실행 중..."
python check_environment.py

echo ""
echo "=========================================="
echo "설정 완료!"
echo "=========================================="
echo ""
echo "다음 단계:"
echo "1. FASTQ 파일들을 data/reads/ 폴더에 복사"
echo "2. 참조 게놈 파일을 data/ref_sequence_genB.fa에 복사"
echo "3. python main.py 실행"
echo ""
echo "데이터 파일 준비가 완료되면 다음 명령어로 실행하세요:"
echo "  source venv/bin/activate"
echo "  python main.py"
echo ""
