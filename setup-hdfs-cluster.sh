#!/bin/bash

# HDFS 클러스터 설정 스크립트
# hongsik1: NameNode
# hongsik2, hongsik3, hongsik4: DataNode들

echo "=== HDFS 클러스터 설정 ==="

# 1. hongsik1에서 NameNode 시작
echo "=== hongsik1에서 NameNode 시작 ==="
ssh -o StrictHostKeyChecking=no kimhongs@hongsik1.vm.informatik.hu-berlin.de 'bash -lc "cd ~/spark-genome-pipeline && docker run --rm -v $(pwd)/hadoop-data:/opt/hadoop/data -v $(pwd)/hadoop-logs:/opt/hadoop/logs ec6a7cf0a97a bash -c \"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

# core-site.xml 설정
cat > /opt/hadoop/etc/hadoop/core-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://0.0.0.0:9000</value>
    </property>
</configuration>
EOF

# hdfs-site.xml 설정
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
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

# NameNode 시작
/opt/hadoop/bin/hdfs namenode &
NN_PID=\$!

# NameNode 시작 대기
sleep 10
echo \"NameNode가 시작되었습니다. PID: \$NN_PID\"
\""'

# 2. hongsik2에서 DataNode 시작
echo "=== hongsik2에서 DataNode 시작 ==="
ssh -o StrictHostKeyChecking=no kimhongs@hongsik2.vm.informatik.hu-berlin.de 'bash -lc "cd ~/spark-genome-pipeline && docker run --rm -v $(pwd)/hadoop-data:/opt/hadoop/data -v $(pwd)/hadoop-logs:/opt/hadoop/logs ec6a7cf0a97a bash -c \"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

# core-site.xml 설정
cat > /opt/hadoop/etc/hadoop/core-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://0.0.0.0:9000</value>
    </property>
</configuration>
EOF

# hdfs-site.xml 설정
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/opt/hadoop/data/datanode</value>
    </property>
</configuration>
EOF

# 디렉토리 생성
mkdir -p /opt/hadoop/data/datanode

# DataNode 시작
/opt/hadoop/bin/hdfs datanode &
DN_PID=\$!

# DataNode 시작 대기
sleep 5
echo \"hongsik2 DataNode가 시작되었습니다. PID: \$DN_PID\"
\""'

# 3. hongsik3에서 DataNode 시작
echo "=== hongsik3에서 DataNode 시작 ==="
ssh -o StrictHostKeyChecking=no kimhongs@hongsik3.vm.informatik.hu-berlin.de 'bash -lc "cd ~/spark-genome-pipeline && docker run --rm -v $(pwd)/hadoop-data:/opt/hadoop/data -v $(pwd)/hadoop-logs:/opt/hadoop/logs ec6a7cf0a97a bash -c \"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

# core-site.xml 설정
cat > /opt/hadoop/etc/hadoop/core-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://0.0.0.0:9000</value>
    </property>
</configuration>
EOF

# hdfs-site.xml 설정
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/opt/hadoop/data/datanode</value>
    </property>
</configuration>
EOF

# 디렉토리 생성
mkdir -p /opt/hadoop/data/datanode

# DataNode 시작
/opt/hadoop/bin/hdfs datanode &
DN_PID=\$!

# DataNode 시작 대기
sleep 5
echo \"hongsik3 DataNode가 시작되었습니다. PID: \$DN_PID\"
\""'

# 4. hongsik4에서 DataNode 시작
echo "=== hongsik4에서 DataNode 시작 ==="
ssh -o StrictHostKeyChecking=no kimhongs@hongsik4.vm.informatik.hu-berlin.de 'bash -lc "cd ~/spark-genome-pipeline && docker run --rm -v $(pwd)/hadoop-data:/opt/hadoop/data -v $(pwd)/hadoop-logs:/opt/hadoop/logs ec6a7cf0a97a bash -c \"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

# core-site.xml 설정
cat > /opt/hadoop/etc/hadoop/core-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://0.0.0.0:9000</value>
    </property>
</configuration>
EOF

# hdfs-site.xml 설정
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << \"EOF\"
<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/opt/hadoop/data/datanode</value>
    </property>
</configuration>
EOF

# 디렉토리 생성
mkdir -p /opt/hadoop/data/datanode

# DataNode 시작
/opt/hadoop/bin/hdfs datanode &
DN_PID=\$!

# DataNode 시작 대기
sleep 5
echo \"hongsik4 DataNode가 시작되었습니다. PID: \$DN_PID\"
\""'

echo "=== HDFS 클러스터 설정 완료 ==="
echo "NameNode: hongsik1.vm.informatik.hu-berlin.de:9000"
echo "DataNodes: hongsik2, hongsik3, hongsik4"



