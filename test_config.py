#!/usr/bin/env python3
"""
Config 경로 설정 테스트 스크립트
"""

from config import Config
from utils import check_tool_availability
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_config_paths():
    """Config의 경로들을 테스트합니다."""
    print("=== Config 경로 테스트 ===")
    print(f"BIGWIG_PATH: {Config.BIGWIG_PATH}")
    
    # BIGWIG_PATH가 문자열인 경우 which 명령어로 확인
    import subprocess
    try:
        result = subprocess.run(['which', Config.BIGWIG_PATH], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"BIGWIG_PATH found at: {result.stdout.strip()}")
            print(f"BIGWIG_PATH exists: True")
        else:
            print(f"BIGWIG_PATH exists: False")
    except Exception as e:
        print(f"BIGWIG_PATH check error: {e}")
    
    print("\n=== 도구 가용성 테스트 ===")
    bigwig_available = check_tool_availability("bedGraphToBigWig", Config.BIGWIG_PATH)
    print(f"bedGraphToBigWig 사용 가능: {bigwig_available}")
    
    print("\n=== 다른 경로들 ===")
    print(f"DATA_DIR: {Config.DATA_DIR}")
    print(f"DATA_DIR exists: {Config.DATA_DIR.exists()}")
    print(f"RESULTS_DIR: {Config.RESULTS_DIR}")
    print(f"RESULTS_DIR exists: {Config.RESULTS_DIR.exists()}")

if __name__ == "__main__":
    test_config_paths()
