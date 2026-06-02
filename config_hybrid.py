import os
from pathlib import Path

class HybridConfig:
    # HDFS 경로 설정
    HDFS_BASE = "hdfs://hongsik1.vm.informatik.hu-berlin.de:9000"
    # 루트(예: /genome)에 쓰기 권한이 없으면: export HDFS_DATA_NAMESPACE=/user/본인id/genome
    _HDFS_DATA_NS = os.environ.get("HDFS_DATA_NAMESPACE", "/genome")
    if not _HDFS_DATA_NS.startswith("/"):
        _HDFS_DATA_NS = "/" + _HDFS_DATA_NS
    HDFS_DATA_DIR = f"{HDFS_BASE}{_HDFS_DATA_NS}"
    HDFS_READS_DIR = f"{HDFS_DATA_DIR}/reads"
    HDFS_REFERENCE_GENOME = f"{HDFS_DATA_DIR}/ref_sequence_genB.fa"
    HDFS_REFERENCE_INDEX = f"{HDFS_DATA_DIR}/ref_sequence_genB.fa.fai"

    # 전체 workflow 중간/최종 산출(HDFS) — 도구(pysam 등)는 로컬 temp 후 업로드
    HDFS_WORKFLOW = f"{HDFS_DATA_DIR}/workflow"
    HDFS_WORK_TRIM = f"{HDFS_WORKFLOW}/trimmed"
    HDFS_WORK_SAM = f"{HDFS_WORKFLOW}/sam"
    HDFS_WORK_BAM = f"{HDFS_WORKFLOW}/bam"
    HDFS_WORK_COVERAGE = f"{HDFS_WORKFLOW}/coverage"

    # hybrid 모듈에서 Path 기대하는 이름과 호환 (HDFS URL 문자열)
    REFERENCE_GENOME = HDFS_REFERENCE_GENOME
    REFERENCE_INDEX = HDFS_REFERENCE_INDEX

    # 로컬 경로 (참조용)
    LOCAL_DATA_DIR = Path("data")
    LOCAL_READS_DIR = LOCAL_DATA_DIR / "reads"
    LOCAL_REFERENCE_GENOME = LOCAL_DATA_DIR / "ref_sequence_genB.fa"
    LOCAL_REFERENCE_INDEX = LOCAL_DATA_DIR / "ref_sequence_genB.fa.fai"

    # 로컬: 로그·JSON 요약·UDF 임시 작업(도구 I/O)만 — 최종 파이프라인 산출은 HDFS_WORK_* 및 HDFS_RESULTS_DIR
    TEMP_DIR = LOCAL_DATA_DIR / "temp"
    # 레거시 코드 호환(로컬만 필요한 UDF 내부 temp): 실질적 최종 경로는 HDFS
    RESULTS_DIR = LOCAL_DATA_DIR / "temp" / "hybrid_staging"
    
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
    MAX_MEMORY = "6g"  # driver / executor 기본 메모리
    
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
        """로컬에 필요한 디렉터리를 생성합니다 (HDFS 경로는 별도 hdfs dfs -mkdir)."""
        for directory in (cls.LOCAL_DATA_DIR, cls.TEMP_DIR, cls.RESULTS_DIR):
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
