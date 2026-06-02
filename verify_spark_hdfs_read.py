#!/usr/bin/env python3
"""JVM( Spark FileSystem)으로 HDFS defaultFS·경로 읽기 가능 여부를 짧게 확인."""
import os
import sys
from pathlib import Path

# 프로젝트 루트
ROOT = Path(__file__).resolve().parent
os.environ.setdefault("HADOOP_CONF_DIR", str(ROOT / "hadoop-config"))
sys.path.insert(0, str(ROOT))

from config_hybrid import HybridConfig as C  # noqa: E402

from pyspark.sql import SparkSession  # noqa: E402


def main() -> int:
    path = (sys.argv[1] if len(sys.argv) > 1 else C.HDFS_READS_DIR).rstrip("/")
    spark = (
        SparkSession.builder.appName("verify-hdfs-read")
        .master(os.environ.get("SPARK_MASTER", "local[1]"))
        .getOrCreate()
    )
    try:
        jvm = spark.sparkContext._jvm
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
        p = jvm.org.apache.hadoop.fs.Path(path)
        if not fs.exists(p):
            print("FAIL: path does not exist (Spark FS):", path)
            return 2
        st = fs.getFileStatus(p)
        print("OK: Spark sees HDFS path:", path)
        print("    isDir=", st.isDirectory(), "size=", st.getLen() if st.isFile() else "N/A")
        # 목록(최대 5개)
        it = fs.listStatus(p)
        for i, s in enumerate(it[:5]):
            print("   ", s.getPath().getName())
        if len(it) > 5:
            print("    ... and", len(it) - 5, "more")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
