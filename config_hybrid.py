import os
from pathlib import Path

class HybridConfig:
    # HDFS 경로 설정
    HDFS_BASE = "hdfs://hongsik1.vm.informatik.hu-berlin.de:9000"
    
    # 데이터 경로 (HDFS)
    HDFS_DATA_DIR = f"{HDFS_BASE}/genome"
    HDFS_READS_DIR = f"{HDFS_DATA_DIR}/reads"
    HDFS_REFERENCE_GENOME = f"{HDFS_DATA_DIR}/ref_sequence_genB.fa"
    HDFS_REFERENCE_INDEX = f"{HDFS_DATA_DIR}/ref_sequence_genB.fa.fai"
    
    # 로컬 경로 (참조용)
    LOCAL_DATA_DIR = Path("data")
    LOCAL_READS_DIR = LOCAL_DATA_DIR / "reads"
    LOCAL_REFERENCE_GENOME = LOCAL_DATA_DIR / "ref_sequence_genB.fa"
    LOCAL_REFERENCE_INDEX = LOCAL_DATA_DIR / "ref_sequence_genB.fa.fai"
    
    # 결과 저장 경로 (HDFS)
    HDFS_RESULTS_DIR = f"{HDFS_DATA_DIR}/results/hybrid_pipeline"
    HDFS_TEMP_DIR = f"{HDFS_DATA_DIR}/temp"
    
    # Spark 설정 (클러스터 모드)
    SPARK_MASTER = "spark://141.20.38.81:7077"
    SPARK_APP_NAME = "HybridGenomeAnalysisPipeline"
    
    # 도구 경로 (시스템에 설치된 도구들)
    FASTP_PATH = "fastp"
    BWA_PATH = "bwa"
    SAMTOOLS_PATH = "samtools"
    BEDTOOLS_PATH = "bedtools"
    BIGWIG_PATH = "bedGraphToBigWig"
    
    # 파이프라인 설정
    PARTITION_SIZE = 500000  # 파티션 크기 감소 (더 빠른 처리)
    MAX_MEMORY = "700g"  # 시스템 메모리 대부분 사용 (genCov와 동일)
    
    # SAM 처리 설정
    SAM_PROCESSING_CONFIG = {
        "filter_quality": 0,       # 필터링 없음 (Original과 동일)
        "remove_duplicates": False # 중복 제거 없음 (Original과 동일)
    }
    
    # 커버리지 설정
    COVERAGE_CONFIG = {
        "window_size": 1000        # 커버리지 윈도우 크기
    }
    
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
            # 성능 최적화 설정 (genCov와 동일한 조건)
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            # 직렬화 최적화
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            # 메모리 최적화 (더 적극적)
            "spark.memory.fraction": "0.8",  # 더 적극적
            "spark.memory.storageFraction": "0.3",  # 더 적극적
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "512MB",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "16",  # 병렬도 증가
            # I/O 최적화
            "spark.sql.adaptive.forceApply": "true",
            "spark.sql.adaptive.logLevel": "INFO",
            # Python UDF 최적화
            "spark.sql.execution.pyspark.udf.faulthandler.enabled": "true",
            "spark.python.worker.faulthandler.enabled": "true",
            # 추가 성능 최적화
            "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "100MB"
        }
