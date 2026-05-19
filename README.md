<div align="center">
  <h1>Big Data Project - Hadoop MapReduce and Apache Spark</h1>

![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

*Big Data analysis on a football players dataset using MapReduce (Hadoop) and Apache Spark, comparing the two frameworks across single-node and cluster execution modes.*

[📚 Overview](#-overview) • [🗂️ Project Structure](#️-project-structure) • [⚙️ Build & Run](#️-build--run)
</div>

---

## 📚 Overview

This project implements a Big Data analysis pipeline on a World Cup football players dataset (`players.csv`), using two different distributed computing frameworks: **Hadoop MapReduce** and **Apache Spark**. The analysis computes the number of goals and penalties scored by substitute (bench) players for each nation, providing a statistic that highlights the impact of substitutions across national teams.

Both implementations are run and compared across two execution modes: **single-node** and **multi-node cluster**, the latter orchestrated via Docker. This allows a direct comparison of the two frameworks in terms of ease of development and execution performance.

---

## 🗂️ Project Structure

```text
Big-Data-Project/
├── MapReduce/
│ ├── MapReduce/
│ │ └── src/mapreduce/
│ │ ├── DriverWorldCup.java # Job configuration and entry point
│ │ ├── MapperWorldCup.java # Mapper: filters substitute players' goals
│ │ └── ReducerWorldCup.java # Reducer: aggregates goal counts by nation
│ ├── MapReduce.jar # Compiled MapReduce job
│ ├── Prompt_commands.txt # Docker + Hadoop setup and run commands
│ ├── players.csv # Input dataset
│ └── output/ # MapReduce output
├── Spark/
│ ├── Spark/
│ │ └── src/it/unisa/hpc/worldcup/
│ │ └── WorldCupDriver.java # Spark job entry point
│ ├── Spark.jar # Compiled Spark job
│ ├── launch_single.sh # Launch script for single-node mode
│ ├── launch_cluster.sh # Launch script for cluster mode
│ ├── players.csv # Input dataset
│ ├── outputSingle/ # Spark single-node output
│ └── outputCluster/ # Spark cluster output
└── Big_Data_Report.pdf # Full project report
└── LICENSE
```

---

## ⚙️ Build & Run

### MapReduce (Hadoop + Docker)

Start the Hadoop cluster using Docker, then copy the dataset to HDFS and submit the job:

```bash
docker image build -t hadoop-new .
docker network create --driver bridge hadoop_network
docker compose up -d
docker container exec -ti master bash

hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

hdfs dfs -put players.csv hdfs:///input
hadoop jar MapReduce.jar /input /output
hdfs dfs -cat /output/part-r-00000 > result.txt
```

### Spark — Single Node

```bash
bash Spark/launch_single.sh
```

### Spark — Cluster Mode

```bash
bash Spark/launch_cluster.sh
```

The cluster script submits the job to a Spark master at `spark://spark-master:7077` with 1G executor memory, reading from `./input` and writing results to `./outputCluster`.

The full methodology and results are documented in [Big_Data_Report.pdf](Big_Data_Report.pdf).
