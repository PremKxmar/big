# 🏙️ Smart City Real-Time Traffic Simulation & Predictive Analytics

> **Final Year Big Data Project** — Distributed processing of **7+ GB / 46M+ NYC taxi trips** with multi-model ML, real-time streaming, and interactive dashboard.

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![Spark](https://img.shields.io/badge/PySpark-4.0.1-orange.svg)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-red.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

---

## 📋 Project Overview

A complete **Big Data Analytics pipeline** that processes NYC Taxi trip data to predict traffic congestion in real time:

| Metric | Value |
|--------|-------|
| **Raw Data** | 7+ GB (4 CSV files, 2015-01 to 2016-03) |
| **Total Trips** | 46,000,000+ |
| **Grid Cells** | 327 unique traffic zones |
| **Best Model** | GBT + OneVsRest — **79.24% accuracy** |
| **Streaming Throughput** | 1,000+ events/second via Kafka |
| **Dashboard Refresh** | Every 5 seconds (WebSocket) |

---

## 🏗️ Architecture

```
┌──────────────┐     ┌──────────────┐     ┌───────────────────────────────┐
│  NYC Taxi    │     │   Apache     │     │   Apache Spark (PySpark 4.0)  │
│  CSV (7 GB)  │────▶│   Kafka      │────▶│   • Data Cleaning             │
│              │     │              │     │   • Feature Engineering        │
│  HDFS        │     │  taxi-trips  │     │   • RDD Analysis (46M rows)   │
│  (9.9 GB)    │     │   topic      │     │   • MLlib Multi-Model (RF/    │
│              │     │              │     │     GBT+OneVsRest/LR)         │
└──────────────┘     └──────────────┘     └───────────────────────────────┘
       │                    │                          │
       ▼                    ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Flask API (port 5000) + React Dashboard + Prometheus/Grafana          │
│  Real-time congestion visualization • Spark MLlib model serving        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🧰 Technology Stack

| Layer | Technology |
|-------|-----------|
| **Batch Processing** | PySpark 4.0.1 (DataFrame + RDD + MLlib) |
| **Streaming** | Apache Kafka → Spark Structured Streaming |
| **Storage** | HDFS (Hadoop 3.2.1 via Docker), Parquet |
| **ML** | Spark MLlib — RF, GBT+OneVsRest, Logistic Regression |
| **Preprocessing** | Python (PySpark) + Scala (Dataset API) |
| **API** | Flask + Flask-SocketIO + Prometheus metrics |
| **Frontend** | React + TypeScript + Leaflet maps |
| **Infrastructure** | Docker Compose (13 containers) |
| **Monitoring** | Prometheus + Grafana |

---

## 📁 Project Structure

```
SmartCityTrafficSystem/
├── backend/
│   ├── docker-compose.yml              # HDFS, Kafka, Spark, Prometheus, Grafana
│   ├── run_pipeline_local.py           # Master pipeline orchestrator
│   ├── src/
│   │   ├── batch/
│   │   │   ├── data_cleaning_spark.py        # PySpark cleaning (local + HDFS)
│   │   │   ├── feature_engineering_spark.py  # Window/Agg features
│   │   │   ├── model_training_spark.py       # Multi-model comparison (RF/GBT/LR)
│   │   │   ├── traffic_rdd_analysis.py       # RDD API on 46M real records
│   │   │   └── hdfs_utils.py                 # HDFS operations
│   │   ├── scala/
│   │   │   └── TrafficDataPreprocessor.scala # Scala Dataset API preprocessing
│   │   ├── streaming/
│   │   │   ├── kafka_producer.py             # Trip event producer
│   │   │   ├── spark_streaming_consumer.py   # Spark Structured Streaming
│   │   │   ├── kafka_api_bridge.py           # API bridge consumer
│   │   │   └── streaming_e2e_test.py         # End-to-end streaming test
│   │   ├── api/
│   │   │   └── app.py                        # Flask REST API + WebSocket
│   │   └── config/
│   │       └── spark_config.py               # Centralized Spark/HDFS config
│   ├── models/
│   │   ├── spark_congestion_model/           # GBT+OneVsRest PipelineModel
│   │   ├── model_info_spark.json             # Production model metadata
│   │   ├── model_comparison.json             # RF vs GBT vs LR results
│   │   └── feature_columns_spark.json        # 16 feature names
│   └── monitoring/
│       ├── prometheus/prometheus.yml
│       └── grafana/
├── data/
│   ├── raw/            # 4 CSV files (7+ GB)
│   ├── processed/      # Cleaned Parquet files
│   └── readable/       # Human-readable exports
├── frontend/           # React + TypeScript dashboard
├── docs/               # IEEE paper, guides, documentation
├── ML_MODEL_STATISTICS_REPORT.md
├── TECHNICAL_REQUIREMENTS_CHECKLIST.md
└── PROJECT_REVIEW.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+, Java 17+, Docker Desktop, 16 GB+ RAM

### 1. Start Infrastructure
```powershell
cd backend
docker-compose up -d    # 13 containers: HDFS, Kafka, Spark, Prometheus, Grafana
```

### 2. Run Batch Pipeline
```powershell
# Option A: Master orchestrator (runs everything)
python run_pipeline_local.py

# Option B: Step by step
python src/batch/data_cleaning_spark.py          # ~600s
python src/batch/feature_engineering_spark.py     # ~68s
python src/batch/model_training_spark.py          # ~542s (3 models)
python src/batch/traffic_rdd_analysis.py          # ~1104s (RDD on 46M rows)
```

### 3. Start API + Dashboard
```powershell
python src/api/app.py        # Flask on http://localhost:5000
cd ../frontend && npm run dev # React on http://localhost:3000
```

### 4. Web UIs
| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 |
| API Health | http://localhost:5000/api/health |

---

## 🤖 ML Model Results

3-model comparison on **499,817 samples** (temporal split: Jan-Feb train, March test):

| Model | Accuracy | F1 Score | Training Time |
|-------|----------|----------|---------------|
| Random Forest (100 trees) | 78.56% | 0.768 | 61.6s |
| **GBT + OneVsRest** ⭐ | **79.24%** | **0.780** | **165.0s** |
| Logistic Regression | 76.50% | 0.742 | 6.5s |

**Production model**: GBT + OneVsRest (best accuracy & F1)

---

## 📊 Dataset

**Source**: NYC Taxi & Limousine Commission (TLC)

| File | Size | Records |
|------|------|---------|
| yellow_tripdata_2015-01.csv | 1.89 GB | ~12M trips |
| yellow_tripdata_2016-01.csv | 1.63 GB | ~11M trips |
| yellow_tripdata_2016-02.csv | 1.70 GB | ~11M trips |
| yellow_tripdata_2016-03.csv | 1.83 GB | ~12M trips |
| **Total** | **~7 GB** | **~46M trips** |

---

## 📄 License

MIT License — Free to use for educational purposes.
