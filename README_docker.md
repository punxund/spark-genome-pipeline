## Docker-based setup for Spark Hybrid Genome Pipeline

### Prerequisites
- 4 VMs: `hongsik1` (master + driver), `hongsik2-4` (workers)
- NFS mounted at `/mnt/genome` on all VMs
- Docker and docker-compose installed

### 1) Build the image (on hongsik1)
```bash
cd /vol/fob-vol7/mi21/kimhongs/spark-genome-pipeline
docker build -t spark-bio:latest .
```

You can also push to a registry and pull on workers if desired.

### 2) Start Spark master (on hongsik1)
```bash
docker compose -f docker-compose.master.yml up -d
# Master URL will be: spark://hongsik1.vm.informatik.hu-berlin.de:7077
```

### 3) Start Spark workers (on hongsik2-4)
```bash
docker compose -f docker-compose.worker.yml up -d
# Optional: override cores/memory
# WORKER_CORES=6 WORKER_MEMORY=4G docker compose -f docker-compose.worker.yml up -d
```

### 4) Run the pipeline (from hongsik1)
```bash
docker exec -it spark-master bash -lc "\
  spark-submit \
  --master spark://hongsik1.vm.informatik.hu-berlin.de:7077 \
  --deploy-mode client \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=6 \
  --conf spark.driver.memory=4g \
  --conf spark.sql.adaptive.enabled=true \
  /workspace/pipeline/main_hybrid.py \
  --reads-dir /mnt/genome/reads \
  --reference-genome /mnt/genome/refs/ref.fa \
  --reference-index /mnt/genome/refs/ref.fa.fai \
  --spark-master spark://hongsik1.vm.informatik.hu-berlin.de:7077"
```

### Notes
- Ensure fastp, bwa, bedtools, samtools are in the image (provided by Dockerfile).
- Results are written under `/mnt/genome/results` (shared NFS), so they are centralized.
- Master UI: `http://hongsik1.vm.informatik.hu-berlin.de:8080`


