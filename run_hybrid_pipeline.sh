#!/bin/bash

# Spark 유전체 분석 파이프라인 - Hybrid 버전 실행 스크립트
# Python 라이브러리 기반 도구들을 활용한 파이프라인

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

# 도움말 함수
show_help() {
    echo "Spark 유전체 분석 파이프라인 - Hybrid 버전"
    echo ""
    echo "사용법: $0 [옵션]"
    echo ""
    echo "옵션:"
    echo "  -r, --reads-dir DIR        읽기 파일 디렉토리 경로 (필수)"
    echo "  -g, --reference-genome FILE 참조 게놈 파일 경로 (필수)"
    echo "  -i, --reference-index FILE 참조 인덱스 파일 경로 (필수)"
    echo "  -m, --spark-master URL     Spark 마스터 URL (기본값: local[*])"
    echo "  -h, --help                 이 도움말 메시지 표시"
    echo ""
    echo "예시:"
    echo "  $0 -r /path/to/reads -g /path/to/reference.fa -i /path/to/reference.fa.fai"
    echo "  $0 --reads-dir /path/to/reads --reference-genome /path/to/reference.fa --reference-index /path/to/reference.fa.fai --spark-master spark://localhost:7077"
    echo ""
    echo "필요한 외부 도구:"
    echo "  - fastp: FASTQ 전처리"
    echo "  - BWA: 읽기 매핑"
    echo ""
    echo "필요한 Python 라이브러리:"
    echo "  - pysam: SAM/BAM 파일 처리"
    echo "  - pybedtools: BED 파일 처리"
    echo "  - pybigwig: BigWig 파일 생성"
}

# 의존성 확인 함수
check_dependencies() {
    log_info "의존성 확인 중..."
    
    # Python 라이브러리 확인
    python3 -c "import pysam" 2>/dev/null || {
        log_error "pysam 라이브러리가 설치되지 않았습니다."
        log_info "다음 명령으로 설치하세요: pip install pysam"
        exit 1
    }
    
    python3 -c "import pybedtools" 2>/dev/null || {
        log_error "pybedtools 라이브러리가 설치되지 않았습니다."
        log_info "다음 명령으로 설치하세요: pip install pybedtools"
        exit 1
    }
    
    python3 -c "import pybigwig" 2>/dev/null || {
        log_error "pybigwig 라이브러리가 설치되지 않았습니다."
        log_info "다음 명령으로 설치하세요: pip install pybigwig"
        exit 1
    }
    
    # 외부 도구 확인
    command -v fastp >/dev/null 2>&1 || {
        log_error "fastp 도구가 설치되지 않았습니다."
        exit 1
    }
    
    command -v bwa >/dev/null 2>&1 || {
        log_error "BWA 도구가 설치되지 않았습니다."
        exit 1
    }
    
    log_success "모든 의존성이 확인되었습니다."
}

# 메인 함수
main() {
    # 기본값 설정
    READS_DIR=""
    REFERENCE_GENOME=""
    REFERENCE_INDEX=""
    SPARK_MASTER="local[*]"
    
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
    
    # 필수 인수 확인
    if [[ -z "$READS_DIR" ]]; then
        log_error "읽기 파일 디렉토리 경로가 필요합니다."
        show_help
        exit 1
    fi
    
    if [[ -z "$REFERENCE_GENOME" ]]; then
        log_error "참조 게놈 파일 경로가 필요합니다."
        show_help
        exit 1
    fi
    
    if [[ -z "$REFERENCE_INDEX" ]]; then
        log_error "참조 인덱스 파일 경로가 필요합니다."
        show_help
        exit 1
    fi
    
    # 파일 존재 확인
    if [[ ! -d "$READS_DIR" ]]; then
        log_error "읽기 파일 디렉토리를 찾을 수 없습니다: $READS_DIR"
        exit 1
    fi
    
    if [[ ! -f "$REFERENCE_GENOME" ]]; then
        log_error "참조 게놈 파일을 찾을 수 없습니다: $REFERENCE_GENOME"
        exit 1
    fi
    
    if [[ ! -f "$REFERENCE_INDEX" ]]; then
        log_error "참조 인덱스 파일을 찾을 수 없습니다: $REFERENCE_INDEX"
        exit 1
    fi
    
    # 의존성 확인
    check_dependencies
    
    # 파이프라인 실행
    log_info "Spark 유전체 분석 파이프라인 - Hybrid 버전 시작"
    log_info "읽기 디렉토리: $READS_DIR"
    log_info "참조 게놈: $REFERENCE_GENOME"
    log_info "참조 인덱스: $REFERENCE_INDEX"
    log_info "Spark 마스터: $SPARK_MASTER"
    
    # Python 파이프라인 실행
    python3 main_hybrid.py \
        --reads-dir "$READS_DIR" \
        --reference-genome "$REFERENCE_GENOME" \
        --reference-index "$REFERENCE_INDEX" \
        --spark-master "$SPARK_MASTER"
    
    if [[ $? -eq 0 ]]; then
        log_success "파이프라인이 성공적으로 완료되었습니다!"
    else
        log_error "파이프라인 실행 중 오류가 발생했습니다."
        exit 1
    fi
}

# 스크립트 실행
main "$@"








