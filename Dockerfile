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

# Install Apache Hadoop (using pre-downloaded file)
ARG HADOOP_VERSION=3.3.1
COPY hadoop-${HADOOP_VERSION}.tar.gz /tmp/
RUN tar -xz -C /opt -f /tmp/hadoop-${HADOOP_VERSION}.tar.gz \
 && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm /tmp/hadoop-${HADOOP_VERSION}.tar.gz

# Install Apache Spark (using pre-downloaded file)
# Use Spark 3.5.1 with the hadoop3 classifier (works with Hadoop 3.x clients)
ARG SPARK_VERSION=3.5.1
ARG HADOOP_CLASSIFIER=3
COPY spark-${SPARK_VERSION}-bin-hadoop${HADOOP_CLASSIFIER}.tgz /tmp/
RUN tar -xz -C /opt -f /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_CLASSIFIER}.tgz \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_CLASSIFIER} /opt/spark \
 && rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_CLASSIFIER}.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV SPARK_HOME=/opt/spark
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Configure Hadoop to use the correct JAVA_HOME
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

WORKDIR /workspace

# Default command does nothing; actual role defined by docker-compose
CMD ["bash", "-lc", "tail -f /dev/null"]


