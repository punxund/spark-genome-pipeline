import os
from pathlib import Path

class PyToolsConfigOptimized:
    """Python 네이티브 도구들을 위한 최적화된 설정 - 성능 중심"""
    
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
    MAX_MEMORY = "800g"  # 최대 메모리
    MAX_CORES = 72  # 시스템의 최대 코어 수
    
    # Python 도구별 설정
    FASTP_CONFIG = {
        "threads": 16,  # 스레드 수
        "compression": 6,
        "qualified_quality_phred": 10,  # 품질 임계값
        "length_required": 30,  # 최소 길이
        "low_complexity_filter": False  # 복잡도 필터 비활성화
    }
    
    ALIGNMENT_CONFIG = {
        "algorithm": "bwa",  # 정렬 알고리즘
        "threads": 24,  # 스레드 수
        "memory_limit": "64g"  # 메모리 제한
    }
    
    SAM_PROCESSING_CONFIG = {
        "threads": 16,  # 스레드 수
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
        """최적화된 Spark 설정을 반환합니다 - 성능 중심"""
        # Python 경로 설정
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        python_path = f"{current_dir}:{parent_dir}"
        
        return {
            # === 핵심 Spark 설정 ===
            "spark.master": cls.SPARK_MASTER,
            "spark.app.name": cls.SPARK_APP_NAME,
            "spark.driver.memory": cls.MAX_MEMORY,
            "spark.executor.memory": cls.MAX_MEMORY,
            
            # === 메모리 최적화 ===
            "spark.memory.fraction": "0.8",                    # 실행 메모리 비율
            "spark.memory.storageFraction": "0.3",             # 저장 메모리 비율
            "spark.python.worker.memory": "400g",              # Python 워커 메모리
            
            # === 병렬 처리 최적화 ===
            "spark.default.parallelism": str(cls.MAX_CORES * 2),      # 기본 병렬 처리 수
            "spark.sql.shuffle.partitions": str(cls.MAX_CORES * 2),   # 셔플 파티션 수
            
            # === 적응형 쿼리 최적화 ===
            "spark.sql.adaptive.enabled": "true",                      # 적응형 쿼리 활성화
            "spark.sql.adaptive.coalescePartitions.enabled": "true",   # 파티션 병합 최적화
            "spark.sql.adaptive.skewJoin.enabled": "true",            # 데이터 기울어진 조인 최적화
            "spark.sql.adaptive.localShuffleReader.enabled": "true",   # 로컬 셔플 리더
            
            # === 파티션 관리 ===
            "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": str(cls.MAX_CORES * 4),
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
            
            # === 데이터 기울어진 조인 처리 ===
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
            "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold": "0",
            
            # === 직렬화 및 I/O 최적화 ===
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",  # Kryo 직렬화
            "spark.sql.adaptive.forceApply": "true",
            
            # === Python 최적화 ===
            "spark.python.worker.python": "python3",
            "spark.python.worker.reuse": "true",                           # 워커 재사용
            "spark.python.worker.pythonpath": python_path,                  # Python 경로
            
            # === Arrow 최적화 ===
            "spark.sql.execution.arrow.pyspark.enabled": "true",           # Arrow 활성화
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",       # Arrow 배치 크기
            
            # === Parquet 최적화 ===
            "spark.sql.parquet.compression.codec": "snappy",               # 빠른 압축
            "spark.sql.parquet.enableVectorizedReader": "true",            # 벡터화 리더
            "spark.sql.parquet.filterPushdown": "true",                    # 필터 푸시다운
            "spark.sql.parquet.aggregatePushdown": "true",                 # 집계 푸시다운
            "spark.sql.parquet.columnarReaderBatchSize": "8192",           # 읽기 배치 크기
            
            # === 파일 시스템 최적화 ===
            "spark.sql.files.maxPartitionBytes": "128MB",                  # 파티션당 최대 바이트
            "spark.sql.files.ignoreCorruptFiles": "true",                  # 손상된 파일 무시
            "spark.sql.files.ignoreMissingFiles": "true",                  # 누락된 파일 무시
            
            # === 브로드캐스트 조인 최적화 ===
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "100MB",      # 자동 브로드캐스트 조인 임계값
            
            # === 로깅 및 모니터링 ===
            "spark.sql.adaptive.logLevel": "INFO",
            "spark.ui.port": "4040",
            "spark.ui.enabled": "true",
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "/tmp/spark-events"
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

# 기존 클래스와의 호환성을 위한 별칭
PyToolsConfig = PyToolsConfigOptimized

