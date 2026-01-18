# Smart City Traffic System - Big Data Project Review

## 📋 Project Overview

This document provides a comprehensive explanation of the **Smart City Traffic System** - a Big Data Analytics project that analyzes NYC Taxi trip data to predict traffic congestion using distributed computing technologies.

---

## 🏗️ Architecture Overview

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                  SMART CITY TRAFFIC                     │
                    │           Big Data Analytics Architecture               │
                    └─────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────────────────────────┐
│   HDFS       │     │   KAFKA      │     │   SPARK                          │
│   (Storage)  │     │  (Streaming) │     │   (Processing)                   │
├──────────────┤     ├──────────────┤     ├──────────────────────────────────┤
│              │     │              │     │  Local Mode: local[*]            │
│  Raw CSV     │────▶│ taxi-trips   │────▶│  • Data Cleaning                 │
│  Parquet     │     │   topic      │     │  • Feature Engineering           │
│  Features    │     │              │     │  • ML Training (MLlib)           │
│  Models      │     │              │     │  • Streaming Processing          │
└──────────────┘     └──────────────┘     └──────────────────────────────────┘
       │                    │                          │
       ▼                    ▼                          ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                    FRONTEND DASHBOARD (React)                           │
  │                Real-time Congestion Visualization                       │
  └─────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Big Data Components Implemented

### 1. Hadoop HDFS (Hadoop Distributed File System)

#### **Implementation Status**: ✅ Fully Implemented

HDFS is used for distributed storage of raw data, processed data, features, and trained models.

#### **Docker Configuration** (`backend/docker-compose.yml`)
- **NameNode Container**: Manages filesystem metadata
  - Web UI Port: `9870`
  - HDFS Port: `9000`
- **DataNode Container**: Stores actual data blocks
  - Web UI Port: `9864`
  - Data Transfer Port: `9866`
- Cluster Name: `smart-city-traffic`
- Replication Factor: 1 (for development)

#### **How to View Data in HDFS Dashboard**

1. **Start the HDFS containers:**
   ```powershell
   cd c:\sem6-real\vscode2\SmartCityTrafficSystem\backend
   docker-compose up -d namenode datanode
   ```

2. **Wait ~30 seconds for initialization, then access:**
   - **HDFS Web UI**: http://localhost:9870
   - Navigate to **Utilities → Browse the file system**
   
3. **HDFS Directory Structure:**
   ```
   hdfs://localhost:9000/smart-city-traffic/
   ├── data/
   │   ├── raw/                           # Raw CSV files
   │   │   ├── yellow_tripdata_2015-01/
   │   │   ├── yellow_tripdata_2016-01/
   │   │   ├── yellow_tripdata_2016-02/
   │   │   └── yellow_tripdata_2016-03/
   │   ├── processed/                     # Cleaned Parquet files
   │   │   ├── yellow_tripdata_2015-01_clean.parquet/
   │   │   ├── yellow_tripdata_2016-01_clean.parquet/
   │   │   └── ...
   │   ├── features/                      # ML training features
   │   │   └── training_features_spark.parquet/
   │   └── models/                        # Trained Spark MLlib model
   │       └── spark_congestion_model/
   ```

4. **List HDFS contents via command line:**
   ```powershell
   python src/batch/hdfs_utils.py list
   ```

#### **HDFS Utilities Module** (`backend/src/batch/hdfs_utils.py`)
Available commands:
```powershell
python src/batch/hdfs_utils.py setup     # Create HDFS directories
python src/batch/hdfs_utils.py upload    # Upload local CSV to HDFS
python src/batch/hdfs_utils.py list      # List HDFS contents
python src/batch/hdfs_utils.py health    # Check HDFS health status
python src/batch/hdfs_utils.py download  # Download from HDFS to local
```

---

### 2. Apache Spark - Mode of Execution

#### **Spark Mode Used**: 🔵 **Local Mode** (`local[*]`)

The project primarily uses **Spark Local Mode** for all batch processing operations. This is configured in all Spark scripts:

```python
# From data_cleaning_spark.py, feature_engineering_spark.py, model_training_spark.py
spark = builder.master("local[*]").getOrCreate()
```

#### **What `local[*]` Means:**
- `local` = Run Spark locally on the same machine (not on a cluster)
- `[*]` = Use all available CPU cores for parallelism
- This provides multi-threaded parallel processing on a single machine

#### **Comparison of Spark Modes:**

| Mode | Configuration | Description | Our Project |
|------|--------------|-------------|-------------|
| **Local Mode** | `local[*]` | Runs on single machine with multiple threads | ✅ **Used** |
| **Cluster Mode** | `spark://master:7077` | Runs on distributed cluster, driver on cluster | ⚠️ Available in Docker |
| **Client Mode** | `--deploy-mode client` | Runs on cluster, driver on client machine | ❌ Not used |

#### **Cluster Mode Availability:**
The `docker-compose.yml` does include Spark cluster containers (1 Master + 2 Workers) that can be used for cluster mode:
```
spark-master:  Port 8081 (Web UI), Port 7077 (Spark Master)
spark-worker-1: Port 8082 (Web UI)
spark-worker-2: Port 8083 (Web UI)
```

To submit to cluster mode (if needed):
```powershell
python src/batch/spark_submit.py all --cluster --hdfs
```

#### **Why Local Mode Was Chosen:**
1. Simpler development and debugging
2. Sufficient for the dataset size (~7GB)
3. No network overhead
4. Easier to demonstrate for reviews
5. Uses the same Spark APIs as cluster mode (code is portable)

---

### 3. Scala Preprocessing

#### **Implementation Status**: ✅ Fully Implemented

#### **File Location**: `backend/src/scala/TrafficDataPreprocessor.scala`

This is a complete 406-line Scala implementation that mirrors the Python preprocessing functionality using Spark's Dataset API.

#### **How to Find/View the Scala Code:**
```
c:\sem6-real\vscode2\SmartCityTrafficSystem\backend\src\scala\
├── TrafficDataPreprocessor.scala  (406 lines - Main preprocessing)
└── build.sbt                       (SBT build configuration)
```

#### **Key Scala Features Implemented:**

1. **Schema Definition:**
   ```scala
   def getTaxiSchema(): StructType = {
     StructType(Array(
       StructField("VendorID", IntegerType, true),
       StructField("tpep_pickup_datetime", StringType, true),
       // ... 17 more fields
     ))
   }
   ```

2. **Data Transformations:**
   ```scala
   // Filter valid NYC coordinates
   def filterValidCoordinates(df: DataFrame): DataFrame = {
     df.filter(
       col("pickup_lat").between(NYC_LAT_MIN, NYC_LAT_MAX) &&
       col("pickup_lon").between(NYC_LON_MIN, NYC_LON_MAX)
     )
   }
   
   // Calculate derived metrics
   def calculateDerivedMetrics(df: DataFrame): DataFrame = {
     df.withColumn("duration_seconds",
         unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))
       .withColumn("speed_mph", round(col("trip_distance") / col("duration_hours"), 2))
   }
   ```

3. **Grid Cell Creation:**
   ```scala
   def createGridCells(df: DataFrame): DataFrame = {
     df.withColumn("cell_lat",
         ((col("pickup_lat") - lit(NYC_LAT_MIN)) / lit(CELL_SIZE)).cast(IntegerType))
       .withColumn("cell_id", concat_ws("_", lit("cell"), col("cell_lat"), col("cell_lon")))
   }
   ```

#### **How to Run Scala Preprocessing:**
```bash
# Option 1: Spark Submit
spark-submit --class TrafficDataPreprocessor target/scala-2.12/traffic-preprocessor.jar

# Option 2: From Spark Shell
:load src/scala/TrafficDataPreprocessor.scala
TrafficDataPreprocessor.main(Array())
```

#### **Scala Output Location:**
```
c:\sem6-real\vscode2\smart-city-traffic\data\processed\scala_cleaned.parquet
c:\sem6-real\vscode2\smart-city-traffic\data\processed\scala_features.parquet
```

---

### 4. Python Preprocessing (PySpark)

#### **Implementation Status**: ✅ Fully Implemented

#### **File Locations:**
```
c:\sem6-real\vscode2\SmartCityTrafficSystem\backend\src\batch\
├── data_cleaning_spark.py       # Data cleaning (426 lines)
├── feature_engineering_spark.py  # Feature engineering (523 lines)
├── model_training_spark.py       # ML training (513 lines)
└── hdfs_utils.py                 # HDFS utilities (342 lines)
```

#### **Processed Data Location (Local):**
```
c:\sem6-real\vscode2\SmartCityTrafficSystem\data\processed\
├── yellow_tripdata_2015-01_clean.parquet/  # Cleaned 2015 Q1 data
├── yellow_tripdata_2016-01_clean.parquet/  # Cleaned 2016 Jan
├── yellow_tripdata_2016-02_clean.parquet/  # Cleaned 2016 Feb
├── yellow_tripdata_2016-03_clean.parquet/  # Cleaned 2016 Mar
├── training_features_spark.parquet/        # ML features
├── training_features.parquet               # Alternative format
└── feature_columns_spark.json              # Feature column list
```

---

### 5. Spark MLlib (Machine Learning)

#### **Implementation Status**: ✅ Fully Implemented

#### **MLlib Components Used:**

| Component | Purpose |
|-----------|---------|
| `VectorAssembler` | Combines feature columns into feature vector |
| `StandardScaler` | Normalizes features (mean=0, std=1) |
| `RandomForestClassifier` | Main classification algorithm (100 trees) |
| `GBTClassifier` | Alternative: Gradient Boosted Trees |
| `Pipeline` | Orchestrates ML workflow |
| `MulticlassClassificationEvaluator` | Evaluates accuracy, precision, recall, F1 |
| `CrossValidator` | Hyperparameter tuning |
| `ParamGridBuilder` | Creates parameter grid for tuning |

#### **ML Pipeline Configuration:**
```python
# VectorAssembler - combines 17 features
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_raw")

# StandardScaler - normalizes features
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

# RandomForestClassifier
rf = RandomForestClassifier(
    numTrees=100,
    maxDepth=10,
    minInstancesPerNode=10,
    featureSubsetStrategy="sqrt",
    seed=42
)

# Pipeline
pipeline = Pipeline(stages=[assembler, scaler, rf])
```

#### **Model Output Location:**
```
c:\sem6-real\vscode2\SmartCityTrafficSystem\backend\models\
├── spark_congestion_model/          # Saved Spark MLlib model
├── model_info_spark.json            # Model metadata & metrics
└── feature_columns_spark.json       # Feature column names
```

---

### 6. Apache Kafka (Streaming)

#### **Implementation Status**: ✅ Fully Implemented

#### **Docker Configuration:**
- **Zookeeper**: Port `2181` (Kafka coordination)
- **Kafka Broker**: Port `9092` (client), `29092` (internal)
- **Kafka UI**: Port `8080` (Web interface)

#### **Kafka Topics:**
| Topic | Description |
|-------|-------------|
| `taxi-trips` | Raw taxi trip events (JSON) |
| `traffic-predictions` | Aggregated congestion predictions |

#### **Streaming Components:**
- `kafka_producer.py` - Streams events to Kafka
- `kafka_api_bridge.py` - API bridge for real-time data
- `spark_streaming_consumer.py` - Consumes from Kafka with Spark Structured Streaming

---

## 📁 Complete Project File Structure

```
SmartCityTrafficSystem/
├── backend/
│   ├── docker-compose.yml           # HDFS, Kafka, Spark cluster
│   ├── HDFS_PIPELINE_README.md      # HDFS usage guide
│   ├── STREAMING_PIPELINE_README.md # Streaming guide
│   ├── src/
│   │   ├── batch/
│   │   │   ├── data_cleaning_spark.py      # PySpark data cleaning
│   │   │   ├── feature_engineering_spark.py # Feature creation
│   │   │   ├── model_training_spark.py      # MLlib training
│   │   │   ├── hdfs_utils.py               # HDFS operations
│   │   │   └── spark_submit.py             # Cluster submit utility
│   │   ├── scala/
│   │   │   ├── TrafficDataPreprocessor.scala # Scala preprocessing
│   │   │   └── build.sbt                    # Scala build config
│   │   ├── streaming/
│   │   │   ├── kafka_producer.py            # Data producer
│   │   │   ├── kafka_api_bridge.py          # API bridge
│   │   │   └── spark_streaming_consumer.py  # Spark streaming
│   │   ├── api/
│   │   │   └── app.py                       # Flask REST API
│   │   └── config/
│   │       └── settings.py                  # Configuration
│   ├── models/                              # Trained models
│   └── data/
│       └── processed/                       # Processed parquet files
├── data/
│   ├── raw/                                 # Raw CSV (external)
│   └── processed/                           # Cleaned data
├── frontend/                                # React dashboard
└── docs/                                    # Documentation
```

---

## 🚀 How to Run the Pipeline

### **Step 1: Start Docker Services**
```powershell
cd c:\sem6-real\vscode2\SmartCityTrafficSystem\backend
docker-compose up -d
```

### **Step 2: Setup HDFS & Upload Data**
```powershell
python src/batch/hdfs_utils.py setup
python src/batch/hdfs_utils.py upload
```

### **Step 3: Run Batch Pipeline**
```powershell
# With HDFS
python src/batch/data_cleaning_spark.py --hdfs
python src/batch/feature_engineering_spark.py --hdfs
python src/batch/model_training_spark.py --hdfs

# OR Local mode (without --hdfs flag)
python src/batch/data_cleaning_spark.py
python src/batch/feature_engineering_spark.py
python src/batch/model_training_spark.py
```

### **Step 4: Start the Application**
```powershell
# Backend API
python src/api/app.py

# Frontend (new terminal)
cd ..\frontend
npm run dev
```

---

## 📊 Web UI Dashboards

| Service | URL | Description |
|---------|-----|-------------|
| **HDFS NameNode** | http://localhost:9870 | Browse HDFS files |
| **HDFS DataNode** | http://localhost:9864 | DataNode status |
| **Kafka UI** | http://localhost:8080 | Topics & messages |
| **Spark Master** | http://localhost:8081 | Cluster overview |
| **Spark Worker 1** | http://localhost:8082 | Worker status |
| **Spark Worker 2** | http://localhost:8083 | Worker status |
| **Application** | http://localhost:3000 | Traffic dashboard |

---

## 📈 Technical Summary Table

| Requirement | Status | Implementation Details |
|-------------|--------|------------------------|
| **HDFS** | ✅ Complete | Docker containers, Web UI at :9870, hdfs_utils.py |
| **Spark Mode** | ✅ Local[*] | All cores, same APIs as cluster mode |
| **Scala Preprocessing** | ✅ Complete | TrafficDataPreprocessor.scala (406 lines) |
| **Python Preprocessing** | ✅ Complete | PySpark DataFrame API |
| **Spark MLlib** | ✅ Complete | RandomForest, Pipeline, Evaluators |
| **Kafka Streaming** | ✅ Complete | Producer, Consumer, Spark Structured Streaming |
| **MapReduce Pattern** | ✅ Complete | Implemented via Spark DataFrame (groupBy, agg) |

---

## 🎯 Key Points for Review

1. **Dataset in HDFS**: Access via http://localhost:9870 → Utilities → Browse File System → `/smart-city-traffic/data/`

2. **Scala Preprocessing Location**: `backend/src/scala/TrafficDataPreprocessor.scala`

3. **Spark Mode**: **Local Mode** (`local[*]`) - Uses all CPU cores on single machine

4. **Processed Data Location (Local)**: `data/processed/` directory contains parquet files

5. **ML Model Location**: `backend/models/spark_congestion_model/`

6. **Why Local Mode?**: Simpler for development, sufficient for dataset size, same APIs as cluster mode (portable code)

---

*Document generated: January 8, 2026*
*Project: Smart City Traffic System - Big Data Analysis*



[
  "hour",
  "day_of_week", 
  "month",
  "is_weekend",
  "is_rush_hour",
  "is_night",
  "cell_lat",
  "cell_lon",
  "is_manhattan_int",
  "prev_trip_count",
  "prev_avg_speed",
  "prev_congestion_label",
  "prev_2h_trip_count",
  "prev_2h_avg_speed",
  "historical_avg_trips",
  "historical_avg_speed"
]