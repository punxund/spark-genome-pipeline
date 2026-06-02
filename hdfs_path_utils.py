"""
HDFS 경로: 로컬 작업(도구) 후 hdfs dfs -get / -put.
실행기(executor)에 `hdfs` CLI(Hadoop client)가 PATH에 있어야 합니다.
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# 호스트/컨테이너: `HADOOP_HDFS_WRAPPER` / `hadoop fs` / `hdfs dfs` 순으로 사용
def _cmd_base() -> List[str]:
    if os.environ.get("HADOOP_HDFS_WRAPPER"):
        return os.environ["HADOOP_HDFS_WRAPPER"].split()
    if shutil.which("hdfs"):
        return ["hdfs", "dfs"]
    if shutil.which("hadoop"):
        return ["hadoop", "fs"]
    return ["hdfs", "dfs"]


def is_hdfs_uri(s: str) -> bool:
    return isinstance(s, str) and s.startswith("hdfs://")


def _ensure_hadoop_cli() -> None:
    if os.environ.get("HADOOP_HDFS_WRAPPER"):
        return
    if shutil.which("hdfs") or shutil.which("hadoop"):
        return
    raise RuntimeError(
        "Hadoop HDFS CLI not found: install a client and add `hdfs` or `hadoop` to PATH, "
        "or set HADOOP_HDFS_WRAPPER (e.g. `export HADOOP_HDFS_WRAPPER='hdfs dfs'`)."
    )


def hdfs_run(args: list, check: bool = False) -> subprocess.CompletedProcess:
    _ensure_hadoop_cli()
    cmd = _cmd_base() + args
    logger.debug("Running: %s", " ".join(cmd))
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=check,
    )


def hdfs_mkdir_p(hdfs_path: str) -> bool:
    r = hdfs_run(["-mkdir", "-p", hdfs_path], check=False)
    if r.returncode != 0:
        logger.warning("hdfs_mkdir_p failed: %s — %s", hdfs_path, r.stderr)
        return False
    return True


def hdfs_get(hdfs_src: str, local_dest: Path) -> bool:
    local_dest.parent.mkdir(parents=True, exist_ok=True)
    r = hdfs_run(["-get", "-f", hdfs_src, str(local_dest)], check=False)
    if r.returncode != 0:
        logger.error("hdfs_get failed: %s -> %s — %s", hdfs_src, local_dest, r.stderr)
        return False
    return True


def hdfs_put(local_src: Path, hdfs_dest: str) -> bool:
    r = hdfs_run(["-put", "-f", str(local_src), hdfs_dest], check=False)
    if r.returncode != 0:
        logger.error("hdfs_put failed: %s -> %s — %s", local_src, hdfs_dest, r.stderr)
        return False
    return True


def hdfs_file_size(hdfs_path: str) -> Optional[int]:
    r = hdfs_run(["-du", "-s", hdfs_path], check=False)
    if r.returncode != 0 or not r.stdout.strip():
        return None
    parts = r.stdout.split()
    if not parts:
        return None
    try:
        return int(parts[0])
    except ValueError:
        return None


def hdfs_ls(hdfs_dir: str) -> List[str]:
    r = hdfs_run(["-ls", hdfs_dir], check=False)
    if r.returncode != 0:
        return []
    out = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        parts = line.split()
        if len(parts) >= 1:
            out.append(parts[-1])
    return out


def parse_fastq_pairs_hdfs(hdfs_dir: str) -> List[Tuple[str, str, str]]:
    """HDFS 읽기 디렉터리에서 *_1.fastq / *_2.fastq 쌍 (경로는 hdfs:// URI)."""
    paths = hdfs_ls(hdfs_dir)
    r1_by_sample: dict = {}
    for p in paths:
        name = p.rsplit("/", 1)[-1]
        m = re.match(r"^(.+)_1\.fastq(\.gz)?$", name)
        if m:
            r1_by_sample[m.group(1)] = p
    pairs: List[Tuple[str, str, str]] = []
    for sample_id, r1 in r1_by_sample.items():
        n2a = f"{sample_id}_2.fastq"
        n2b = f"{sample_id}_2.fastq.gz"
        for p in paths:
            bn = p.rsplit("/", 1)[-1]
            if bn == n2a or bn == n2b:
                pairs.append((sample_id, r1, p))
                break
    return pairs


def local_scratch(prefix: str = "hybrid_") -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def download_if_hdfs(uri: str, work_dir: Path) -> str:
    """hdfs면 work_dir에 받아 로컬 경로(str) 반환. 아니면 uri 그대로."""
    if not is_hdfs_uri(uri):
        return uri
    name = uri.rstrip("/").rsplit("/", 1)[-1]
    local = work_dir / name
    if not hdfs_get(uri, local):
        raise OSError(f"Could not HDFS get: {uri}")
    return str(local)


def download_reference_fasta_and_index(hdfs_fa: str, work_dir: Path) -> str:
    """
    HDFS의 ref.fa 및 BWA 인덱스(.amb .ann .bwt .pac .sa)와 .fai를 work_dir에 받습니다.
    로컬 ref FASTA 경로를 반환합니다.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    fa_name = hdfs_fa.rstrip("/").rsplit("/", 1)[-1]
    local_fa = work_dir / fa_name
    if not hdfs_get(hdfs_fa, local_fa):
        raise OSError(f"hdfs_get failed for reference: {hdfs_fa}")
    for suf in (".amb", ".ann", ".bwt", ".pac", ".sa"):
        hp = hdfs_fa + suf
        lp = work_dir / (fa_name + suf)
        if not hdfs_get(hp, lp):
            logger.info("Optional ref artifact missing (bwa may index): %s", hp)
    lp_fai = work_dir / (fa_name + ".fai")
    if not hdfs_get(hdfs_fa + ".fai", lp_fai):
        logger.info("Optional .fai missing: %s", hdfs_fa + ".fai")
    return str(local_fa)


def put_file_to_dir(local_file: Path, hdfs_dir: str) -> str:
    """hdfs_dir(디렉터리)에 파일 업로드 후 전체 hdfs URI 반환."""
    name = local_file.name
    dest = hdfs_dir.rstrip("/") + "/" + name
    if not hdfs_mkdir_p(hdfs_dir):
        pass
    if not hdfs_put(local_file, dest):
        raise OSError(f"Could not HDFS put: {local_file} -> {dest}")
    return dest


def ensure_hybrid_hdfs_workflowDirs(cfg) -> None:
    for d in (
        cfg.HDFS_WORKFLOW,
        cfg.HDFS_WORK_TRIM,
        cfg.HDFS_WORK_SAM,
        cfg.HDFS_WORK_BAM,
        cfg.HDFS_WORK_COVERAGE,
        cfg.HDFS_RESULTS_DIR,
        cfg.HDFS_TEMP_DIR,
    ):
        hdfs_mkdir_p(d)
        logger.info("HDFS dir OK: %s", d)
