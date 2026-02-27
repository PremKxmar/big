# 🏙️ Smart City Real-Time Traffic Simulation & Predictive Analytics

> A Distributed Streaming Pipeline with GeoSpatial Analysis and Deep Learning

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![Spark](https://img.shields.io/badge/PySpark-4.0.1-orange.svg)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-red.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## 📋 Project Overview

This project simulates and predicts city traffic in near-real-time using a 3D interactive dashboard. It processes **7+ GB of NYC Taxi trip data** (45+ million trips) through a complete big-data pipeline featuring:

- **Real-time streaming** of vehicle positions via Kafka
- **Distributed processing** with Apache Spark
- **ML-powered predictions** for traffic congestion
- **3D visualization** with Kepler.gl

## 🎯 Problem Statement & 5 V's

| Dimension | Implementation |
|-----------|----------------|
| **Volume** | 7+ GB of NYC Taxi data (45M+ trips from 2015-2016) |
| **Velocity** | 1,000+ vehicle events/second via Kafka streaming |
| **Variety** | GPS coordinates, timestamps, trip metrics, road cells |
| **Veracity** | Data cleaning, outlier removal, missing value handling |
| **Value** | Predict congestion 15 min ahead, optimize routes |

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  NYC Taxi CSV   │────▶│   Apache Spark  │────▶│   HDFS/Parquet  │
│  (7+ GB Raw)    │     │   (Batch ETL)   │     │   (Processed)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Kafka Producer │────▶│ Spark Streaming │────▶│   ML Scoring    │
│  (Trip Replay)  │     │  (Real-time)    │     │  (Predictions)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Kepler.gl     │◀────│   Flask API     │◀────│  Kafka Output   │
│  (3D Dashboard) │     │   (Backend)     │     │  (Events)       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## 📁 Project Structure

```
backend/
├── docker-compose.yml              # HDFS, Kafka, Spark, Prometheus, Grafana (13 containers)
├── run_pipeline_local.py           # Master pipeline orchestrator
├── requirements.txt
│
├── src/
│   ├── batch/
│   │   ├── data_cleaning_spark.py        # PySpark cleaning (local + HDFS)
│   │   ├── feature_engineering_spark.py  # Window/Agg features
│   │   ├── model_training_spark.py       # Multi-model (RF/GBT+OneVsRest/LR)
│   │   ├── traffic_rdd_analysis.py       # RDD API on 46M real records
│   │   └── hdfs_utils.py                # HDFS operations
│   │
│   ├── streaming/
│   │   ├── kafka_producer.py             # Trip event producer
│   │   ├── spark_streaming_consumer.py   # Spark Structured Streaming
│   │   ├── kafka_api_bridge.py           # API bridge consumer
│   │   └── streaming_e2e_test.py         # E2E streaming test (802 lines)
│   │
│   ├── api/
│   │   └── app.py                        # Flask REST API + Prometheus metrics
│   │
│   ├── scala/
│   │   └── TrafficDataPreprocessor.scala # Scala Dataset API preprocessing
│   │
│   └── config/
│       └── spark_config.py               # Centralized Spark/HDFS config
│
├── models/
│   ├── spark_congestion_model/           # GBT+OneVsRest PipelineModel
│   ├── model_info_spark.json             # Production model metadata
│   ├── model_comparison.json             # RF vs GBT vs LR results
│   └── feature_columns_spark.json        # 16 feature names
│
├── monitoring/
│   ├── prometheus/prometheus.yml
│   └── grafana/
│
└── data/
    └── processed/                        # Processed Parquet files
```

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Docker Desktop
- 16GB+ RAM recommended

### 1. Setup Environment
```bash
cd smart-city-traffic
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Start Infrastructure
```bash
docker-compose up -d
```

### 3. Run Data Pipeline
```bash
# Option A: Master orchestrator (runs everything)
python run_pipeline_local.py

# Option B: Step by step
python src/batch/data_cleaning_spark.py          # ~600s
python src/batch/feature_engineering_spark.py     # ~68s
python src/batch/model_training_spark.py          # ~542s (3 models)
python src/batch/traffic_rdd_analysis.py          # ~1104s (RDD on 46M rows)
```

### 4. Start Streaming
```bash
# Terminal 1: Kafka producer
python src/streaming/kafka_producer.py

# Terminal 2: Spark streaming consumer
python src/streaming/spark_streaming_consumer.py

# Or run E2E test
python src/streaming/streaming_e2e_test.py
```

### 5. Launch Dashboard
```bash
python src/api/app.py
# Open http://localhost:5000
```

## 📊 Dataset

**Source**: NYC Taxi & Limousine Commission (TLC) Trip Record Data

| File | Size | Records |
|------|------|---------|
| yellow_tripdata_2015-01.csv | 1.89 GB | ~12M trips |
| yellow_tripdata_2016-01.csv | 1.63 GB | ~11M trips |
| yellow_tripdata_2016-02.csv | 1.70 GB | ~11M trips |
| yellow_tripdata_2016-03.csv | 1.83 GB | ~12M trips |
| **Total** | **~7 GB** | **~46M trips** |

## 🤖 Machine Learning

3-model comparison on **499,817 samples** (temporal split: Jan-Feb train, March test):

| Model | Accuracy | F1 Score | Training Time |
|-------|----------|----------|---------------|
| Random Forest (100 trees) | 78.56% | 0.768 | 61.6s |
| **GBT + OneVsRest** ⭐ | **79.24%** | **0.780** | **165.0s** |
| Logistic Regression | 76.50% | 0.742 | 6.5s |

- **Production model**: GBT + OneVsRest (best accuracy & F1)
- **Features**: 16 engineered features (temporal, spatial, historical)
- **Target**: 3-class congestion (Low / Medium / High)
- **Pipeline**: VectorAssembler → StandardScaler → OneVsRest(GBTClassifier)

## 🎨 Visualization Features

- 🚕 Moving taxi dots (colored by speed)
- 🔥 3D hexagon heatmap (congestion density)
- 🔮 Predicted jam zones (15 min ahead)
- ⏱️ Time slider (replay entire day)
- 📊 Real-time KPI dashboard

## 👨‍💻 Author

Final Year Big Data Analytics Project

## 📄 License

MIT License
