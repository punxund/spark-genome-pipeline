#!/bin/bash

# Spark 유전체 분석 파이프라인 실행 스크립트

set -e  # 오류 발생 시 스크립트 중단

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 스크립트 디렉토리 확인
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 기본 설정
READS_DIR="../data/reads"
REFERENCE_GENOME="../data/ref_sequence_genB.fa"
REFERENCE_INDEX="../data/ref_sequence_genB.fa.fai"
SPARK_MASTER="local[*]"
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="4g"
EXECUTOR_CORES="2"

# 도움말 함수
show_help() {
    echo "Spark 유전체 분석 파이프라인 실행 스크립트"
    echo ""
    echo "사용법: $0 [옵션]"
    echo ""
    echo "옵션:"
    echo "  -r, --reads-dir DIR        읽기 파일 디렉토리 (기본값: $READS_DIR)"
    echo "  -g, --reference-genome FILE 참조 게놈 파일 (기본값: $REFERENCE_GENOME)"
    echo "  -i, --reference-index FILE 참조 인덱스 파일 (기본값: $REFERENCE_INDEX)"
    echo "  -m, --spark-master URL     Spark 마스터 URL (기본값: $SPARK_MASTER)"
    echo "  -d, --driver-memory MEM    드라이버 메모리 (기본값: $DRIVER_MEMORY)"
    echo "  -e, --executor-memory MEM  실행자 메모리 (기본값: $EXECUTOR_MEMORY)"
    echo "  -c, --executor-cores NUM   실행자 코어 수 (기본값: $EXECUTOR_CORES)"
    echo "  -h, --help                 이 도움말 표시"
    echo ""
    echo "예시:"
    echo "  $0"
    echo "  $0 --reads-dir /path/to/reads --reference-genome /path/to/ref.fa"
    echo "  $0 -r /path/to/reads -g /path/to/ref.fa -m spark://cluster:7077"
}

# 명령행 인수 파싱
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--reads-dir)
            READS_DIR="$2"
            shift 2
            ;;
        -g|--reference-genome)
            REFERENCE_GENOME="$2"
            shift 2
            ;;
        -i|--reference-index)
            REFERENCE_INDEX="$2"
            shift 2
            ;;
        -m|--spark-master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        -d|--driver-memory)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        -e|--executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        -c|--executor-cores)
            EXECUTOR_CORES="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            log_error "알 수 없는 옵션: $1"
            show_help
            exit 1
            ;;
    esac
done

# 환경 확인
check_environment() {
    log_info "환경 확인 중..."
    
    # Python 확인
    if ! command -v python3 &> /dev/null; then
        log_error "Python3가 설치되지 않았습니다."
        exit 1
    fi
    
    # Spark 확인
    if ! command -v spark-submit &> /dev/null; then
        log_warning "spark-submit을 찾을 수 없습니다. 로컬 Python으로 실행합니다."
        SPARK_AVAILABLE=false
    else
        SPARK_AVAILABLE=true
        log_success "Spark가 사용 가능합니다."
    fi
    
    # Java 확인 (Spark 사용 시)
    if [ "$SPARK_AVAILABLE" = true ]; then
        if ! command -v java &> /dev/null; then
            log_error "Java가 설치되지 않았습니다. Spark 실행에 필요합니다."
            exit 1
        fi
    fi
    
    # 입력 파일 확인
    if [ ! -d "$READS_DIR" ]; then
        log_error "읽기 파일 디렉토리를 찾을 수 없습니다: $READS_DIR"
        exit 1
    fi
    
    if [ ! -f "$REFERENCE_GENOME" ]; then
        log_error "참조 게놈 파일을 찾을 수 없습니다: $REFERENCE_GENOME"
        exit 1
    fi
    
    if [ ! -f "$REFERENCE_INDEX" ]; then
        log_error "참조 인덱스 파일을 찾을 수 없습니다: $REFERENCE_INDEX"
        exit 1
    fi
    
    log_success "환경 확인 완료"
}

# 의존성 확인
check_dependencies() {
    log_info "의존성 도구 확인 중..."
    
    tools=("fastp" "bwa" "samtools" "bedtools")
    missing_tools=()
    
    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        log_error "다음 도구들이 설치되지 않았습니다: ${missing_tools[*]}"
        log_info "다음 명령어로 설치할 수 있습니다:"
        log_info "  sudo apt-get install fastp bwa samtools bedtools"
        log_info "  또는"
        log_info "  conda install -c bioconda fastp bwa samtools bedtools"
        exit 1
    fi
    
    log_success "모든 의존성 도구가 사용 가능합니다"
}

# Python 패키지 확인
check_python_packages() {
    log_info "Python 패키지 확인 중..."
    
    required_packages=("pyspark" "pandas" "numpy" "matplotlib" "seaborn")
    missing_packages=()
    
    for package in "${required_packages[@]}"; do
        if ! python3 -c "import $package" &> /dev/null; then
            missing_packages+=("$package")
        fi
    done
    
    if [ ${#missing_packages[@]} -ne 0 ]; then
        log_error "다음 Python 패키지들이 설치되지 않았습니다: ${missing_packages[*]}"
        log_info "다음 명령어로 설치할 수 있습니다:"
        log_info "  pip install -r requirements.txt"
        exit 1
    fi
    
    log_success "모든 Python 패키지가 사용 가능합니다"
}

# 파이프라인 실행
run_pipeline() {
    log_info "Spark 유전체 분석 파이프라인 시작"
    log_info "설정:"
    log_info "  읽기 디렉토리: $READS_DIR"
    log_info "  참조 게놈: $REFERENCE_GENOME"
    log_info "  참조 인덱스: $REFERENCE_INDEX"
    log_info "  Spark 마스터: $SPARK_MASTER"
    
    if [ "$SPARK_AVAILABLE" = true ]; then
        log_info "Spark를 사용하여 파이프라인을 실행합니다..."
        
        spark-submit \
            --master "$SPARK_MASTER" \
            --driver-memory "$DRIVER_MEMORY" \
            --executor-memory "$EXECUTOR_MEMORY" \
            --executor-cores "$EXECUTOR_CORES" \
            main.py \
            --reads-dir "$READS_DIR" \
            --reference-genome "$REFERENCE_GENOME" \
            --reference-index "$REFERENCE_INDEX" \
            --spark-master "$SPARK_MASTER"
    else
        log_info "로컬 Python으로 파이프라인을 실행합니다..."
        
        python3 main.py \
            --reads-dir "$READS_DIR" \
            --reference-genome "$REFERENCE_GENOME" \
            --reference-index "$REFERENCE_INDEX" \
            --spark-master "$SPARK_MASTER"
    fi
}

# 메인 실행
main() {
    log_info "Spark 유전체 분석 파이프라인 실행 스크립트"
    log_info "시작 시간: $(date)"
    
    # 환경 확인
    check_environment
    
    # 의존성 확인
    check_dependencies
    
    # Python 패키지 확인
    check_python_packages
    
    # 파이프라인 실행
    run_pipeline
    
    log_success "파이프라인 실행 완료"
    log_info "종료 시간: $(date)"
}

# 스크립트 실행
main "$@"
