import subprocess
import logging
import os
from pathlib import Path
from typing import List, Tuple, Optional
import tempfile
import shutil

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(cmd: List[str], cwd: Optional[Path] = None, check: bool = True, suppress_warnings: bool = False) -> subprocess.CompletedProcess:
    """
    명령어를 실행하고 결과를 반환합니다.
    
    Args:
        cmd: 실행할 명령어 리스트
        cwd: 작업 디렉토리
        check: 실패 시 예외 발생 여부
        suppress_warnings: 경고 메시지 억제 여부
    
    Returns:
        subprocess.CompletedProcess 객체
    """
    logger.info(f"실행 중: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd, 
            cwd=cwd, 
            check=check, 
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            logger.info("명령어 실행 성공")
        elif not suppress_warnings:
            logger.warning(f"명령어 실행 실패: {result.stderr}")
        return result
    except subprocess.CalledProcessError as e:
        if not suppress_warnings:
            logger.error(f"명령어 실행 중 오류 발생: {e}")
        if check:
            raise
        return e

def check_tool_availability(tool_name: str, tool_path: str) -> bool:
    """
    도구의 사용 가능 여부를 확인합니다.
    
    Args:
        tool_name: 도구 이름
        tool_path: 도구 경로
    
    Returns:
        사용 가능 여부
    """
    try:
        # 도구별로 다른 확인 방법 사용
        if tool_name == "bwa":
            # bwa는 인자 없이 실행하면 도움말이 나옴
            result = run_command([tool_path], check=False, suppress_warnings=True)
        elif tool_name == "samtools":
            # samtools는 인코딩 문제가 있을 수 있으므로 간단한 확인
            result = run_command([tool_path, "view"], check=False, suppress_warnings=True)
        elif tool_name == "bedGraphToBigWig":
            # bedGraphToBigWig는 인자 없이 실행하면 도움말이 나옴
            result = run_command([tool_path], check=False, suppress_warnings=True)
        else:
            # 다른 도구들은 --version 사용
            result = run_command([tool_path, "--version"], check=False, suppress_warnings=True)
        
        if result.returncode == 0 or (result.returncode != 0 and ("usage" in result.stderr.lower() or "usage" in result.stdout.lower())):
            logger.info(f"{tool_name} 사용 가능: {tool_path}")
            return True
        else:
            logger.warning(f"{tool_name} 사용 불가: {tool_path}")
            return False
    except FileNotFoundError:
        logger.warning(f"{tool_name} 찾을 수 없음: {tool_path}")
        return False

def create_temp_file(suffix: str = "", prefix: str = "temp_") -> Path:
    """
    임시 파일을 생성합니다.
    
    Args:
        suffix: 파일 확장자
        prefix: 파일 접두사
    
    Returns:
        임시 파일 경로
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=suffix, prefix=prefix, delete=False)
    temp_path = Path(temp_file.name)
    temp_file.close()
    return temp_path

def cleanup_temp_files(temp_files: List[Path]):
    """
    임시 파일들을 정리합니다.
    
    Args:
        temp_files: 정리할 임시 파일 경로 리스트
    """
    for temp_file in temp_files:
        try:
            if temp_file.exists():
                temp_file.unlink()
                logger.debug(f"임시 파일 삭제: {temp_file}")
        except Exception as e:
            logger.warning(f"임시 파일 삭제 실패 {temp_file}: {e}")

def parse_fastq_pairs(reads_dir: Path) -> List[Tuple[str, Path, Path]]:
    """
    FASTQ 파일 쌍을 파싱합니다.
    
    Args:
        reads_dir: 읽기 파일 디렉토리
    
    Returns:
        (샘플ID, R1 파일 경로, R2 파일 경로) 튜플 리스트
    """
    fastq_pairs = []
    
    # R1 파일들을 찾습니다
    r1_files = list(reads_dir.glob("*_1.fastq"))
    
    for r1_file in r1_files:
        # 대응하는 R2 파일을 찾습니다
        sample_id = r1_file.stem.replace("_1", "")
        r2_file = reads_dir / f"{sample_id}_2.fastq"
        
        if r2_file.exists():
            fastq_pairs.append((sample_id, r1_file, r2_file))
            logger.info(f"FASTQ 쌍 발견: {sample_id} ({r1_file.name}, {r2_file.name})")
        else:
            logger.warning(f"R2 파일을 찾을 수 없음: {r2_file}")
    
    return fastq_pairs

def get_file_size_mb(file_path: Path) -> float:
    """
    파일 크기를 MB 단위로 반환합니다.
    
    Args:
        file_path: 파일 경로
    
    Returns:
        파일 크기 (MB)
    """
    if file_path.exists():
        return file_path.stat().st_size / (1024 * 1024)
    return 0.0
