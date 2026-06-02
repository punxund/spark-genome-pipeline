import os
from pathlib import Path

class PyToolsConfig:
    """Python 네이티브 도구들을 위한 설정"""
    
    # 데이터 경로
    DATA_DIR = Path("data")
    READS_DIR = DATA_DIR / "reads"
    REFERENCE_GENOME = DATA_DIR / "ref_sequence_genB.fa"
    REFERENCE_INDEX = DATA_DIR / "ref_sequence_genB.fa.fai"
    
    # 결과 저장 경로
    RESULTS_DIR = Path("results/pytools_pipeline")
    TEMP_DIR = Path("temp")
    
    # Spark 설정
    SPARK_MASTER = "local[*]"
    SPARK_APP_NAME = "PyToolsGenomePipeline"
    
    # Python 라이브러리 설정
    PYTHON_TOOLS = {
        "fastp": "fastp",  # Python 버전의 fastp
        "pysam": "pysam",  # SAM/BAM 처리
        "pybigwig": "pyBigWig",  # BigWig 파일 생성
        "numpy": "numpy",  # 수치 계산
        "pandas": "pandas",  # 데이터 처리
        "biopython": "Bio",  # 생물정보학 도구
        "pyspark": "pyspark"  # Spark 처리
    }
    
    # 파이프라인 설정
    PARTITION_SIZE = 1000000  # 각 파티션당 읽기 수
    MAX_MEMORY = "800g"  # 600g에서 800g로 증가
    MAX_CORES = 72  # 시스템의 최대 코어 수
    
    # Python 도구별 설정
    FASTP_CONFIG = {
        "threads": 16,  # 8에서 16으로 증가
        "compression": 6,
        "qualified_quality_phred": 10,  # 15에서 10으로 완화 (더 많은 읽기 통과)
        "length_required": 30,  # 50에서 30으로 완화 (더 짧은 읽기도 허용)
        "low_complexity_filter": False  # True에서 False로 변경 (복잡도 필터 비활성화)
    }
    
    ALIGNMENT_CONFIG = {
        "algorithm": "bwa",  # 또는 "bowtie2", "minimap2"
        "threads": 24,  # 16에서 24로 증가
        "memory_limit": "64g"  # 32g에서 64g로 증가
    }
    
    SAM_PROCESSING_CONFIG = {
        "threads": 16,  # 8에서 16으로 증가
        "compression": 6,
        "filter_quality": 30,
        "remove_duplicates": True
    }
    
    COVERAGE_CONFIG = {
        "window_size": 1000,
        "step_size": 100,
        "min_coverage": 1,
        "normalize": True
    }
    
    @classmethod
    def create_directories(cls):
        """필요한 디렉토리들을 생성합니다."""
        for directory in [cls.RESULTS_DIR, cls.TEMP_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_spark_config(cls):
        """Spark 설정을 반환합니다."""
        # 현재 디렉토리와 pythontools 디렉토리를 Python 경로에 추가
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        python_path = f"{current_dir}:{parent_dir}"
        
        return {
            "spark.master": cls.SPARK_MASTER,
            "spark.app.name": cls.SPARK_APP_NAME,
            "spark.driver.memory": cls.MAX_MEMORY,
            "spark.executor.memory": cls.MAX_MEMORY,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            # Python 도구 최적화 설정
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
            "spark.sql.adaptive.logLevel": "INFO",
            # Python 최적화
            "spark.python.worker.memory": "400g",  # 320g에서 400g로 증가
            "spark.python.worker.python": "python3",
            "spark.python.worker.reuse": "true",  # worker 재사용
            "spark.python.worker.python.worker.reuse": "true",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
            # Python 경로 설정 (모듈 찾기 문제 해결)
            "spark.python.worker.pythonpath": python_path,
            "spark.python.worker.python.worker.pythonpath": python_path,
            "spark.python.worker.python.worker.python.worker.pythonpath": python_path,
            # 코어 및 병렬 처리 최적화
            "spark.default.parallelism": str(cls.MAX_CORES * 2),  # 코어 수의 2배
            "spark.sql.shuffle.partitions": str(cls.MAX_CORES * 2),  # 셔플 파티션 수
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": str(cls.MAX_CORES * 4),  # 초기 파티션 수 증가
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",  # 파티션 크기 조정
            # I/O 최적화 (Parquet 저장 속도 향상)
            "spark.sql.parquet.compression.codec": "snappy",  # 빠른 압축
            "spark.sql.parquet.enableVectorizedReader": "true",
            "spark.sql.parquet.recordLevelFilter.enabled": "true",
            "spark.sql.parquet.columnarReaderBatchSize": "4096",
            "spark.sql.parquet.mergeSchema": "false",
            "spark.sql.parquet.filterPushdown": "true",
            "spark.sql.parquet.aggregatePushdown": "true",
            # Parquet 저장 성능 최적화 (추가)
            "spark.sql.parquet.enableVectorizedWriter": "true",
            "spark.sql.parquet.vectorized.reader.enabled": "true",
            "spark.sql.parquet.vectorized.writer.enabled": "true",
            "spark.sql.parquet.columnarReaderBatchSize": "8192",  # 배치 크기 증가
            "spark.sql.parquet.columnarWriterBatchSize": "8192",  # 쓰기 배치 크기
            "spark.sql.parquet.columnarReader.enabled": "true",
            "spark.sql.parquet.columnarWriter.enabled": "true",
            # 파일 시스템 최적화
            "spark.sql.files.maxPartitionBytes": "128MB",  # 파티션당 최대 바이트
            "spark.sql.files.openCostInBytes": "4194304",  # 파일 열기 비용
            "spark.sql.files.ignoreCorruptFiles": "true",
            "spark.sql.files.ignoreMissingFiles": "true",
            # 캐시 최적화
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "100MB",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
            # 진행상황 표시 개선
            "spark.sql.adaptive.logLevel": "INFO",
            "spark.sql.execution.adaptive.logLevel": "INFO",
            "spark.sql.execution.adaptive.adaptiveSparkPlan.logLevel": "INFO",
            "spark.sql.execution.adaptive.adaptiveSparkPlan.adaptiveSparkPlan.logLevel": "INFO",
            "spark.sql.execution.adaptive.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.logLevel": "INFO",
            # 진행상황 모니터링 설정
            "spark.sql.adaptive.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.logLevel": "INFO",
            "spark.sql.adaptive.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.logLevel": "INFO",
            # UI 및 모니터링 설정
            "spark.ui.port": "4040",
            "spark.ui.enabled": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "/tmp/spark-events",
            "spark.sql.adaptive.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.adaptiveSparkPlan.logLevel": "INFO"
        }
    
    @classmethod
    def check_python_dependencies(cls):
        """Python 의존성 확인"""
        missing_deps = []
        for tool_name, package_name in cls.PYTHON_TOOLS.items():
            try:
                if package_name == "fastp":
                    # fastp는 Python 패키지가 아니므로 건너뛰기
                    print(f"⚠️ {tool_name} ({package_name}) - Python 패키지 아님 (건너뜀)")
                    continue
                __import__(package_name)
                print(f"✅ {tool_name} ({package_name}) - 사용 가능")
            except ImportError:
                missing_deps.append(f"{tool_name} ({package_name})")
                print(f"❌ {tool_name} ({package_name}) - 설치 필요")
        
        if missing_deps:
            print(f"\n설치가 필요한 패키지들:")
            for dep in missing_deps:
                print(f"  pip install {dep.split('(')[1].split(')')[0]}")
            return False
        return True
