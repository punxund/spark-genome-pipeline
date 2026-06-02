#!/bin/bash

# HDFS 설정 및 데이터 업로드 스크립트

echo "=== HDFS 설정 및 데이터 업로드 ==="

docker run --rm -v $(pwd)/hadoop-data:/opt/hadoop/data -v $(pwd)/hadoop-logs:/opt/hadoop/logs -v $(pwd)/data:/workspace/data ec6a7cf0a97a bash -c "
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

# core-site.xml 설정
cat > /opt/hadoop/etc/hadoop/core-site.xml << 'EOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

# hdfs-site.xml 설정
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << 'EOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/opt/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/opt/hadoop/data/datanode</value>
    </property>
</configuration>
EOF

# 디렉토리 생성
mkdir -p /opt/hadoop/data/namenode
mkdir -p /opt/hadoop/data/datanode

# NameNode 포맷
/opt/hadoop/bin/hdfs namenode -format -force

# HDFS 데몬 시작 (백그라운드)
/opt/hadoop/bin/hdfs --daemon start namenode
/opt/hadoop/bin/hdfs --daemon start datanode

# HDFS 시작 대기
sleep 10

# HDFS 상태 확인
echo \"=== HDFS 상태 확인 ===\"
/opt/hadoop/bin/hdfs dfsadmin -report

# 데이터 디렉토리 생성 및 업로드
echo \"=== 데이터 업로드 ===\"
/opt/hadoop/bin/hdfs dfs -mkdir -p /data
/opt/hadoop/bin/hdfs dfs -put /workspace/data/ref_sequence_genB.fa /data/
/opt/hadoop/bin/hdfs dfs -put /workspace/data/reads/SRR30977596_1.fastq /data/
/opt/hadoop/bin/hdfs dfs -put /workspace/data/reads/SRR30977596_2.fastq /data/

# 업로드된 파일 확인
echo \"=== 업로드된 파일 목록 ===\"
/opt/hadoop/bin/hdfs dfs -ls /data

# HDFS 데몬 종료
/opt/hadoop/bin/hdfs --daemon stop datanode
/opt/hadoop/bin/hdfs --daemon stop namenode
"

echo "HDFS 데이터 업로드 완료!"



