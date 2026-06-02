import os
from pathlib import Path

class Config:
    # main.py: Spark에서 HDFS 입력을 쓸 때 CLI 없이 env만으로도 지정 가능
    # 예: export MAIN_PIPELINE_HDFS_READS=hdfs://namenode:9000/genome/reads
    MAIN_PIPELINE_HDFS_READS = os.environ.get("MAIN_PIPELINE_HDFS_READS", "")
    MAIN_PIPELINE_HDFS_REF = os.environ.get("MAIN_PIPELINE_HDFS_REF", "")
    MAIN_PIPELINE_HDFS_FAI = os.environ.get("MAIN_PIPELINE_HDFS_FAI", "")
    # 데이터 경로
    DATA_DIR = Path("data")
    READS_DIR = DATA_DIR / "reads"
    REFERENCE_GENOME = DATA_DIR / "ref_sequence_genB.fa"
    REFERENCE_INDEX = DATA_DIR / "ref_sequence_genB.fa.fai"
    
    # 결과 저장 경로 (기존 파이프라인 전용)
    RESULTS_DIR = Path("results/original_pipeline")
    TEMP_DIR = Path("temp")
    
    # Spark 설정
    SPARK_MASTER = "local[*]"
    SPARK_APP_NAME = "GenomeAnalysisPipeline"
    
    # 도구 경로 (시스템에 설치된 도구들)
    FASTP_PATH = "fastp"
    BWA_PATH = "bwa"
    SAMTOOLS_PATH = "samtools"
    BEDTOOLS_PATH = "bedtools"
    BIGWIG_PATH = "bedGraphToBigWig"
    
    # 파이프라인 설정
    PARTITION_SIZE = 1000000  # 각 파티션당 읽기 수
    MAX_MEMORY = os.environ.get("MAIN_PIPELINE_SPARK_MAX_MEMORY", "600g")
    
    @classmethod
    def create_directories(cls):
        """필요한 디렉토리들을 생성합니다."""
        for directory in [cls.RESULTS_DIR, cls.TEMP_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_spark_config(cls):
        """Spark 설정을 반환합니다."""
        return {
            "spark.master": cls.SPARK_MASTER,
            "spark.app.name": cls.SPARK_APP_NAME,
            "spark.driver.memory": cls.MAX_MEMORY,
            "spark.executor.memory": cls.MAX_MEMORY,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            # 성능 최적화 설정
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "200",
            # 메모리 최적화
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
            "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
            # I/O 최적화
            "spark.sql.adaptive.forceApply": "true",
            "spark.sql.adaptive.logLevel": "INFO"
        }
