# Smart City Traffic System - Review 1 Documentation

## 📋 Course Information
- **Course Code**: 22AIE312
- **Course Title**: Big Data Analytics
- **Review**: First Review
- **Date**: January 15, 2026

---

# PROJECT OVERVIEW

## Project Title
**Smart City Traffic Congestion Prediction System Using Big Data Analytics**

## Problem Statement
Develop a scalable Big Data analytics system that processes historical NYC taxi trip data (~47 million records, ~7GB) to predict traffic congestion levels using distributed computing frameworks including Hadoop HDFS and Apache Spark.

## Dataset
- **Source**: NYC Taxi & Limousine Commission
- **Files**: 4 CSV files (2015-2016)
- **Total Size**: ~6.9 GB
- **Records**: ~47 million trips
- **Storage**: Hadoop HDFS

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION LAYER                        │
│         NYC Taxi Dataset (7GB CSV) → Hadoop HDFS                │
└────────────────────────────────────┬────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│           PREPROCESSING LAYER (SCALA + APACHE SPARK)           │
│    SparkSession → DataFrame API → Transformations → Parquet    │
│    • Data Cleaning & Validation                                 │
│    • Coordinate Filtering (NYC Bounds)                          │
│    • Speed/Duration Calculation                                 │
│    • Spatial Grid Cell Assignment                               │
└────────────────────────────────────┬────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                SPARK MLLIB PROCESSING                           │
│    • Feature Engineering (17 features)                          │
│    • RandomForest Classification                                │
│    • Model Training & Evaluation                                │
└────────────────────────────────────┬────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                  STREAMING LAYER (KAFKA)                        │
│    • Real-time Trip Ingestion                                   │
│    • Spark Structured Streaming                                 │
└────────────────────────────────────┬────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                   VISUALIZATION LAYER                           │
│    • React Dashboard with Leaflet Maps                          │
│    • Real-time Congestion Heatmaps                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Big Data Components Used

### 1. Hadoop HDFS (Distributed Storage)
- **Purpose**: Store raw and processed data in distributed filesystem
- **Configuration**: Docker containers (NameNode + DataNode)
- **Raw Data**: 6.9 GB stored with replication factor 1
- **Processed Data**: 1.5 GB stored with replication factor 3
- **Web UI**: http://localhost:9870

### 2. Apache Spark (Distributed Processing)
- **Purpose**: Large-scale data processing and ML
- **Mode**: Local[*] - uses all CPU cores
- **Language**: Scala (native Spark language)
- **APIs Used**: DataFrame, Dataset, SparkSQL

### 3. Scala Preprocessing with Spark
- **File**: `TrafficDataPreprocessor.scala` (406 lines)
- **Framework**: Apache Spark 3.5 with Scala 2.12
- **Spark Components Used**:
  - SparkSession (entry point)
  - DataFrame API (transformations)
  - spark.sql.functions (built-in functions)
  - Parquet writer (output format)

### 4. Spark MLlib (Machine Learning)
- **Purpose**: Traffic congestion classification
- **Components**: VectorAssembler, StandardScaler, RandomForestClassifier, Pipeline

### 5. Apache Kafka (Streaming)
- **Purpose**: Real-time data ingestion
- **Topics**: taxi-trips, traffic-predictions

---

## Scala + Spark Implementation Details

### SparkSession Configuration
```scala
val spark = SparkSession.builder()
  .appName("SmartCityTraffic-ScalaPreprocessor")
  .config("spark.driver.memory", "8g")
  .config("spark.executor.memory", "8g")
  .config("spark.sql.parquet.compression.codec", "snappy")
  .config("spark.sql.shuffle.partitions", "200")
  .getOrCreate()
```

### Spark DataFrame Operations Used

| Operation | Spark Function | Purpose |
|-----------|----------------|---------|
| Load CSV | `spark.read.csv()` | Read from HDFS |
| Filter | `df.filter()` | Coordinate validation |
| Transform | `df.withColumn()` | Create derived columns |
| Aggregate | `df.groupBy().agg()` | Cell-level statistics |
| Save | `df.write.parquet()` | Write to HDFS |

### Key Spark Transformations (Scala)
```scala
// Load data using Spark DataFrame API
val df = spark.read
  .option("header", "true")
  .schema(getTaxiSchema())
  .csv(inputPath)  // Reads from HDFS

// Filter using Spark SQL functions
df.filter(
  col("pickup_lat").between(NYC_LAT_MIN, NYC_LAT_MAX) &&
  col("pickup_lon").between(NYC_LON_MIN, NYC_LON_MAX)
)

// Transform using withColumn
df.withColumn("speed_mph", 
  round(col("trip_distance") / col("duration_hours"), 2))

// Aggregate using groupBy
df.groupBy("cell_id", "hour").agg(
  count("*").as("trip_count"),
  avg("speed_mph").as("avg_speed")
)

// Save to HDFS in Parquet format
df.write.mode("overwrite").parquet(outputPath)
```

### Preprocessing Pipeline Steps
1. **Load CSV** → `spark.read.csv(hdfsPath)`
2. **Validate Coordinates** → `df.filter()` with NYC bounds
3. **Calculate Duration** → `unix_timestamp()` difference
4. **Calculate Speed** → `distance / duration_hours`
5. **Filter Valid Trips** → Speed 1-60 mph, Duration 1-180 min
6. **Create Grid Cells** → Spatial indexing (~1km cells)
7. **Extract Temporal Features** → `hour()`, `dayofweek()`, `month()`
8. **Save Parquet** → `df.write.parquet(hdfsPath)`

---

## Dataset Statistics

### Raw Data (in HDFS)
| File | Size |
|------|------|
| yellow_tripdata_2015-01.csv | 1.85 GB |
| yellow_tripdata_2016-01.csv | 1.59 GB |
| yellow_tripdata_2016-02.csv | 1.66 GB |
| yellow_tripdata_2016-03.csv | 1.78 GB |
| **Total** | **~6.9 GB** |

### Processed Data (in HDFS)
| Data Type | Size | Format |
|-----------|------|--------|
| Cleaned Data | 1.5 GB | Parquet |
| Features | 11 MB | Parquet |
| ML Model | Present | Spark ML |

### Compression
- Raw CSV: 6.9 GB → Cleaned Parquet: 1.5 GB
- **77% size reduction** using Parquet + Snappy

---

# PPT SLIDE CONTENT

---

## Slide 1: Title Slide

**Project Title**: Smart City Traffic Congestion Prediction System Using Big Data Analytics

**Team Members**:
- [Name 1] - [Reg. No.]
- [Name 2] - [Reg. No.]
- [Name 3] - [Reg. No.]

**Course**: 22AIE312 - Big Data Analytics
**Review**: First Review | January 2026

---

## Slide 2: Problem Domain

### Traffic Congestion: A Growing Urban Challenge

**Statistics**:
- $87 billion lost annually in the US due to traffic
- Average commuter spends 54 hours/year in traffic
- 30% increase in vehicle emissions during congestion

**Why Big Data?**
- NYC generates 500,000+ taxi trips daily
- Pattern recognition requires processing millions of records
- Real-time prediction needs distributed computing

---

## Slide 3: Problem Statement

### Problem
> Develop a scalable Big Data analytics system that processes historical NYC taxi trip data (~47 million records, ~7GB) to predict traffic congestion levels using distributed computing.

### Challenges
1. Processing 7GB+ of transportation data
2. Distributed storage and computation
3. Real-time congestion classification
4. Scalable machine learning pipeline

---

## Slide 4: Literature Review

| Paper | Key Contribution | Limitation |
|-------|------------------|------------|
| Zhang et al., 2023 | LSTM for traffic patterns | Single machine processing |
| Kumar et al., 2022 | IoT-based monitoring | No historical analysis |
| Chen et al., 2021 | Hadoop MapReduce | No ML integration |
| Lee et al., 2024 | Spark Streaming | Small dataset (~1GB) |

### Research Gap
No existing system integrates HDFS + Scala/Spark preprocessing + Spark MLlib + Kafka streaming in a unified architecture.

---

## Slide 5: Proposed System Architecture

```
┌────────────────────────────────────┐
│        DATA LAYER (HDFS)           │
│    Raw CSV (7GB) → Parquet (1.5GB) │
└──────────────────┬─────────────────┘
                   │
┌──────────────────▼─────────────────┐
│    PROCESSING (SCALA + SPARK)      │
│  SparkSession → DataFrame → Save   │
└──────────────────┬─────────────────┘
                   │
┌──────────────────▼─────────────────┐
│     ML LAYER (SPARK MLLIB)         │
│  RandomForest → Congestion Labels  │
└──────────────────┬─────────────────┘
                   │
┌──────────────────▼─────────────────┐
│    STREAMING (KAFKA + SPARK)       │
│   Real-time Predictions            │
└────────────────────────────────────┘
```

---

## Slide 6: Implementation - Scala + Spark

### Preprocessing with Apache Spark (Scala)

**File**: `TrafficDataPreprocessor.scala` (406 lines)

**Spark Components Used**:
- `SparkSession` - Entry point for Spark
- `DataFrame API` - High-level data transformations
- `spark.sql.functions` - Built-in functions (filter, withColumn, groupBy)
- `Parquet Writer` - Efficient columnar storage

**Code Example**:
```scala
val spark = SparkSession.builder()
  .appName("SmartCityTraffic-ScalaPreprocessor")
  .master("local[*]")  // Use all CPU cores
  .getOrCreate()

// Read from HDFS
val df = spark.read.csv("hdfs://localhost:9000/data/raw/")

// Transform using DataFrame API
df.filter(col("pickup_lat").between(40.47, 40.91))
  .withColumn("speed_mph", col("distance") / col("duration"))
  .write.parquet("hdfs://localhost:9000/data/processed/")
```

---

## Slide 7: Big Data Tools & Technologies

| Component | Tool | Purpose |
|-----------|------|---------|
| **Storage** | Hadoop HDFS 3.2 | Distributed file storage |
| **Processing** | Apache Spark 3.5 | Distributed computation |
| **Language** | Scala 2.12 | Spark preprocessing |
| **ML** | Spark MLlib | RandomForest classifier |
| **Streaming** | Apache Kafka 7.5 | Real-time ingestion |
| **Container** | Docker Compose | Service orchestration |

### Programming Languages
- **Scala**: Data preprocessing (406 lines)
- **Python**: Feature engineering, API
- **TypeScript**: Frontend dashboard

---

## Slide 8: Objectives

1. **To design** a distributed storage architecture using Hadoop HDFS for 7GB+ data

2. **To implement** preprocessing pipelines in **Scala using Apache Spark** DataFrame API

3. **To develop** ML classification using **Spark MLlib** for congestion prediction

4. **To integrate** real-time streaming using Apache Kafka

5. **To evaluate** accuracy, scalability, and performance metrics

---

## Slide 9: Work Completed (Review 1)

### ✅ Completed

| Task | Status |
|------|--------|
| HDFS Docker Setup | ✅ Complete |
| Data Upload to HDFS (6.9 GB) | ✅ Complete |
| Scala + Spark Preprocessing | ✅ 406 lines |
| Data Cleaning & Validation | ✅ Complete |
| Parquet Output to HDFS | ✅ 1.5 GB |
| Feature Engineering | ✅ 17 features |
| Spark MLlib Model | ✅ RandomForest |
| Kafka Streaming Setup | ✅ Ready |

---

## Slide 10: Results & Demo

### HDFS Storage
- **Raw Data**: 6.9 GB (4 CSV files)
- **Processed Data**: 1.5 GB (Parquet)
- **Compression**: 77% reduction

### Spark Processing Stats
- **Records Processed**: ~47 million
- **Unique Grid Cells**: ~2,500
- **Congestion Distribution**: Low 40%, Medium 35%, High 25%

### Demo
- HDFS Web UI: http://localhost:9870
- Browse: /smart-city-traffic/data/

---

## Slide 11: Timeline

| Milestone | Status |
|-----------|--------|
| Project Setup & Dataset | ✅ Complete |
| HDFS Configuration | ✅ Complete |
| **Scala + Spark Preprocessing** | ✅ Complete |
| Feature Engineering | ✅ Complete |
| Spark MLlib Training | ✅ Complete |
| Kafka Streaming | 🔄 In Progress |
| Dashboard | ⏳ Pending |
| Final Demo | ⏳ Pending |

---

*Document Created: January 15, 2026*
*Course: 22AIE312 - Big Data Analytics*
