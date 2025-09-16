FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless python3 python3-pip python3-venv \
    curl wget git ca-certificates locales \
    fastp bwa bedtools samtools \
    build-essential zlib1g-dev libbz2-dev liblzma-dev libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8

# Python packages for the hybrid pipeline
RUN python3 -m pip install --no-cache-dir --upgrade pip \
 && python3 -m pip install --no-cache-dir \
    pysam pybedtools pybigwig pyarrow pandas

# Install Apache Spark (binary distribution)
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  | tar -xz -C /opt \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

WORKDIR /workspace

# Default command does nothing; actual role defined by docker-compose
CMD ["bash", "-lc", "tail -f /dev/null"]


