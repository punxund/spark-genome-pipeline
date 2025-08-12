#!/usr/bin/env python3
"""
환경 검사 스크립트
다른 서버에서 실행하기 전에 필요한 환경을 확인합니다.
"""

import sys
import subprocess
import shutil
from pathlib import Path

def check_python_version():
    """Python 버전을 확인합니다."""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python 3.8+ 필요 (현재: {version.major}.{version.minor}.{version.micro})")
        return False
    else:
        print(f"✅ Python 버전: {version.major}.{version.minor}.{version.micro}")
        return True

def check_java():
    """Java 설치를 확인합니다."""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True, check=False)
        if result.returncode == 0:
            print("✅ Java 설치됨")
            return True
        else:
            print("❌ Java 설치되지 않음")
            return False
    except FileNotFoundError:
        print("❌ Java 설치되지 않음")
        return False

def check_tool(tool_name, tool_path=None):
    """도구 설치를 확인합니다."""
    if tool_path is None:
        tool_path = tool_name
    
    if shutil.which(tool_path):
        print(f"✅ {tool_name}: 설치됨")
        return True
    else:
        print(f"❌ {tool_name}: 설치되지 않음")
        return False

def check_data_files():
    """데이터 파일들을 확인합니다."""
    data_dir = Path("data")
    reads_dir = data_dir / "reads"
    reference_file = data_dir / "ref_sequence_genB.fa"
    
    all_good = True
    
    if not data_dir.exists():
        print("❌ data/ 디렉토리가 없습니다")
        all_good = False
    else:
        print("✅ data/ 디렉토리 존재")
    
    if not reads_dir.exists():
        print("❌ data/reads/ 디렉토리가 없습니다")
        all_good = False
    else:
        fastq_files = list(reads_dir.glob("*_1.fastq")) + list(reads_dir.glob("*_2.fastq"))
        if fastq_files:
            print(f"✅ FASTQ 파일 {len(fastq_files)}개 발견")
        else:
            print("❌ FASTQ 파일이 없습니다")
            all_good = False
    
    if not reference_file.exists():
        print("❌ 참조 게놈 파일이 없습니다: data/ref_sequence_genB.fa")
        all_good = False
    else:
        print("✅ 참조 게놈 파일 존재")
    
    return all_good

def check_python_packages():
    """필요한 Python 패키지들을 확인합니다."""
    required_packages = ['pyspark', 'pandas', 'numpy']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}: 설치됨")
        except ImportError:
            print(f"❌ {package}: 설치되지 않음")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n설치 명령어:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

def main():
    """메인 함수"""
    print("=" * 50)
    print("Spark Genome Pipeline 환경 검사")
    print("=" * 50)
    
    checks = []
    
    # 1. Python 버전 확인
    checks.append(check_python_version())
    
    # 2. Java 확인
    checks.append(check_java())
    
    # 3. 생물정보학 도구 확인
    print("\n생물정보학 도구 확인:")
    tools = ['fastp', 'bwa', 'samtools', 'bedtools', 'bedGraphToBigWig']
    for tool in tools:
        checks.append(check_tool(tool))
    
    # 4. Python 패키지 확인
    print("\nPython 패키지 확인:")
    checks.append(check_python_packages())
    
    # 5. 데이터 파일 확인
    print("\n데이터 파일 확인:")
    checks.append(check_data_files())
    
    # 결과 요약
    print("\n" + "=" * 50)
    print("검사 결과 요약")
    print("=" * 50)
    
    passed = sum(checks)
    total = len(checks)
    
    if passed == total:
        print("🎉 모든 검사 통과! 파이프라인을 실행할 수 있습니다.")
        print("\n실행 명령어:")
        print("python main.py")
        return 0
    else:
        print(f"⚠️ {passed}/{total} 검사 통과")
        print("\n해결 방법:")
        print("1. Python 3.8+ 설치")
        print("2. Java 8+ 설치")
        print("3. 생물정보학 도구 설치: ./install_dependencies.sh")
        print("4. Python 패키지 설치: pip install -r requirements.txt")
        print("5. 데이터 파일들을 올바른 위치에 복사")
        return 1

if __name__ == "__main__":
    sys.exit(main())
