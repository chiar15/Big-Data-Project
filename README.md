<div align="center">
  <h1>🐘🔥 Big Data Project — Hadoop MapReduce & Apache Spark</h1>

![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

*Big Data analysis on a World Cup football players dataset using Hadoop MapReduce and Apache Spark, comparing the two frameworks across single-node and cluster execution modes.*

[📚 Overview](#-overview) • [🎯 Analysis Queries](#-analysis-queries) • [🛠️ Technologies](#️-technologies) • [🗂️ Project Structure](#️-project-structure) • [⚙️ Installation & Run](#️-installation--run) • [📖 Documentation](#-documentation)
</div>

---

## 📚 Overview

This project implements two Big Data analysis queries on a World Cup football players dataset (`players.csv`), using two different distributed computing frameworks: **Hadoop MapReduce** and **Apache Spark**. Each framework tackles a different analytical problem, showcasing the strengths of each paradigm.

The Spark implementation is tested in two execution modes — **single-node** and **multi-node cluster** — the latter orchestrated via Docker, reflecting a realistic distributed environment.

---

## 🎯 Analysis Queries

### 🐘 Exercise 1 — Hadoop MapReduce

Compute the **number of goals and penalties scored by substitute (bench) players for each nation**. A player is considered a substitute if their line-up status is marked as `N` in the dataset. Own goals (`OG`) and missed penalties (`MP`) are explicitly excluded from the count.

### 🔥 Exercise 2 — Apache Spark

Find the **team (or teams, in case of ties) that has won the most matches while having the fewest players on the field** at the end of the game (i.e., accumulating the highest number of red cards among winners). The analysis involves computing goals, own goals, penalties, and red cards per team per match, determining match winners, and ranking them by red card count.

---

## 🛠️ Technologies

| Technology | Purpose |
|------------|---------|
| **Hadoop MapReduce** | Distributed batch processing via Mapper + Reducer |
| **Apache Spark** | In-memory distributed data processing via RDDs |
| **Java** | Implementation language for both frameworks |
| **Docker** | Cluster containerization and orchestration |
| **HDFS** | Distributed file system for Hadoop |
| **YARN** | Resource manager for the Hadoop cluster |

---

## 🗂️ Project Structure

```text
Big-Data-Project/
├── MapReduce/
│ ├── MapReduce/
│ │ └── src/mapreduce/
│ │ ├── DriverWorldCup.java # Job configuration and entry point
│ │ ├── MapperWorldCup.java # Filters substitute players' goals/penalties
│ │ └── ReducerWorldCup.java # Aggregates goal counts by nation
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
```
 
---

## 📦 Dataset

The dataset `players.csv` contains records of World Cup football players, with one row per player per match. The key fields used in the analyses are:

| Field | Description |
|-------|-------------|
| `Match ID` | Unique match identifier |
| `Team` | Nation initials |
| `Line-up` | `S` for starter, `N` for substitute |
| `Events` | String encoding match events (`G` = goal, `P` = penalty, `OG` = own goal, `MP` = missed penalty, `R` / `SY` = red card) |

---

## ⚙️ Installation & Run

### Prerequisites

- **Docker** and **Docker Compose**
- **Java JDK 8+**

### Hadoop MapReduce

Start the Hadoop cluster via Docker, upload the dataset to HDFS and submit the job:

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

To clean up after execution:

```bash
hdfs dfs -rm -r hdfs:///input
hdfs dfs -rm -r hdfs:///output
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/stop-yarn.sh
```

### Apache Spark — Single Node

```bash
bash Spark/launch_single.sh
```

### Apache Spark — Cluster Mode

```bash
bash Spark/launch_cluster.sh
```

The cluster script submits the job to a Spark master at `spark://spark-master:7077` with 1G executor memory, reading from `./input` and writing results to `./outputCluster`.

---

## 📖 Documentation

Full implementation details, theoretical background, and execution results are documented in [Big_Data_Report.pdf](Big_Data_Report.pdf).
