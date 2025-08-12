import os
from pathlib import Path

class Config:
    # 데이터 경로
    DATA_DIR = Path("../data")
    READS_DIR = DATA_DIR / "reads"
    REFERENCE_GENOME = DATA_DIR / "ref_sequence_genB.fa"
    REFERENCE_INDEX = DATA_DIR / "ref_sequence_genB.fa.fai"
    
    # 결과 저장 경로
    RESULTS_DIR = Path("../results/spark_pipeline")
    TEMP_DIR = Path("../temp")
    
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
    MAX_MEMORY = "4g"
    
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
            "spark.sql.adaptive.skewJoin.enabled": "true"
        }
