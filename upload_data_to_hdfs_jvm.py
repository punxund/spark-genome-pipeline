#!/usr/bin/env python3
"""
Hadoop FileSystem (JVM)으로 로컬 data → HDFS에 업로드.
hdfs/hadoop CLI 없이 NameNode( core-site / hdfs-site )에 맞게 put.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
os.environ.setdefault("HADOOP_CONF_DIR", str(REPO / "hadoop-config"))

from config_hybrid import HybridConfig as C  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


def _hdfs_qualified_path(hdfs_uri: str, jvm):
    """hdfs://host:port/a/b -> default FS의 Path /a/b (mkdir 시 inode=/ 오인 방지)."""
    s = hdfs_uri.rstrip("/")
    if s.startswith("hdfs://"):
        rest = s.split("://", 1)[1]  # host:port/...
        if "/" in rest:
            p = "/" + rest.split("/", 1)[1]
        else:
            p = "/"
        return jvm.org.apache.hadoop.fs.Path(p)
    return jvm.org.apache.hadoop.fs.Path(s)


def _default_fs(hconf, jvm):
    return jvm.org.apache.hadoop.fs.FileSystem.get(hconf)


def _jcopy_subtree(
    spark: SparkSession, local_dir: Path, hdfs_dir_uri: str, overwrite: bool
) -> int:
    """local_dir 이하의 모든 파일 → hdfs_dir_uri/상대경로."""
    local_dir = local_dir.resolve()
    if not local_dir.is_dir():
        print(f"ERR: not a directory: {local_dir}", file=sys.stderr)
        return 2
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    JPath = jvm.org.apache.hadoop.fs.Path
    base = _hdfs_qualified_path(hdfs_dir_uri, jvm)
    fs = _default_fs(hconf, jvm)
    if not fs.exists(base):
        if not fs.mkdirs(base):
            print(f"ERR: mkdirs failed: {hdfs_dir_uri}", file=sys.stderr)
            return 1
    for f in local_dir.rglob("*"):
        if not f.is_file():
            continue
        rel = f.relative_to(local_dir)
        rel_s = str(rel).replace("\\", "/")
        dst = JPath(base, rel_s)
        parent = dst.getParent()
        if not fs.exists(parent) and not fs.mkdirs(parent):
            print(f"ERR: mkdirs {parent}", file=sys.stderr)
            return 1
        if fs.exists(dst) and not overwrite:
            print("skip", rel_s)
            continue
        print("put", rel_s, "...")
        src = JPath(f"file://{f}")
        try:
            fs.copyFromLocalFile(False, overwrite, src, dst)
        except Exception as e:  # noqa: BLE001
            print(f"FAIL {rel_s}: {e}", file=sys.stderr)
            return 1
    print("OK subtree:", local_dir, "->", hdfs_dir_uri)
    return 0


def _put_one(
    spark: SparkSession, local_file: Path, hdfs_uri: str, overwrite: bool
) -> int:
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm = spark.sparkContext._jvm
    JPath = jvm.org.apache.hadoop.fs.Path
    dst = _hdfs_qualified_path(hdfs_uri, jvm)
    fs = _default_fs(hconf, jvm)
    parent = dst.getParent()
    if not fs.exists(parent) and not fs.mkdirs(parent):
        print(f"ERR: mkdirs {parent}", file=sys.stderr)
        return 1
    if fs.exists(dst) and not overwrite:
        print("skip ref:", local_file.name)
        return 0
    print("put", local_file.name, "->", hdfs_uri)
    src = JPath(f"file://{local_file.resolve()}")
    try:
        fs.copyFromLocalFile(False, overwrite, src, dst)
    except Exception as e:  # noqa: BLE001
        print("FAIL", e, file=sys.stderr)
        return 1
    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="Upload local data/ to HDFS (HybridConfig layout).",
    )
    p.add_argument(
        "--mode",
        choices=["pipeline", "full"],
        default="pipeline",
        help="pipeline: data/reads + ref_sequence_genB.fa*; full: all data/ → .../genome/mirror",
    )
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing HDFS files")
    a = p.parse_args()

    data = (REPO / "data").resolve()
    if not data.is_dir():
        print("ERR: no data dir:", data, file=sys.stderr)
        return 2

    spark = (
        SparkSession.builder.appName("upload-data-hdfs")
        .master(os.environ.get("UPLOAD_SPARK_MASTER", "local[1]"))
        .getOrCreate()
    )
    try:
        if a.mode == "pipeline":
            r = _jcopy_subtree(
                spark, data / "reads", C.HDFS_READS_DIR, a.overwrite
            )
            if r:
                return r
            for f in sorted(data.glob("ref_sequence_genB.fa*")):
                r = _put_one(
                    spark, f, f"{C.HDFS_DATA_DIR.rstrip('/')}/{f.name}", a.overwrite
                )
                if r:
                    return r
            return 0
        return _jcopy_subtree(
            spark, data, f"{C.HDFS_DATA_DIR.rstrip('/')}/mirror", a.overwrite
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
