# Smart City Traffic System: Comprehensive Preprocessing & Spark MLlib Guide

> **Version**: 1.0  
> **Project**: Smart City Traffic Congestion Prediction System  
> **Technologies**: Apache Spark (3.5.0), Scala (2.12.18), PySpark, Spark MLlib, HDFS  

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Dataset Description](#2-dataset-description)
3. [System Architecture](#3-system-architecture)
4. [Data Pipeline Flow](#4-data-pipeline-flow)
5. [Scala Preprocessing Module](#5-scala-preprocessing-module)
6. [PySpark Data Cleaning](#6-pyspark-data-cleaning)
7. [Feature Engineering with Spark](#7-feature-engineering-with-spark)
8. [Model Training with Spark MLlib](#8-model-training-with-spark-mllib)
9. [HDFS Integration](#9-hdfs-integration)
10. [Output Artifacts](#10-output-artifacts)

---

## 1. Project Overview

### 1.1 Problem Statement

Urban traffic congestion is a significant challenge in modern cities, leading to increased commute times, fuel consumption, and environmental pollution. This project implements a **Big Data Analytics pipeline** to predict traffic congestion levels in New York City using historical taxi trip data.

### 1.2 Objectives

- **Real-time Congestion Prediction**: Predict congestion levels (Low/Medium/High) for spatial grid cells
- **Scalable Processing**: Handle millions of taxi trip records using distributed computing
- **No Data Leakage**: Implement proper ML practices with temporal train/test splits
- **HDFS Integration**: Store and process data on Hadoop Distributed File System

### 1.3 Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Distributed Computing | Apache Spark | 3.5.0 |
| Data Preprocessing | Scala | 2.12.18 |
| Feature Engineering | PySpark | 3.5.0 |
| Machine Learning | Spark MLlib | 3.5.0 |
| Storage | HDFS | 3.x |
| Build Tool | SBT | 1.x |

---

## 2. Dataset Description

### 2.1 Data Source

The project uses the **NYC Yellow Taxi Trip Records** dataset from the NYC Taxi and Limousine Commission (TLC). This is a publicly available dataset containing detailed trip records.

**Dataset Location**: `c:\sem6-real\bigdata\vscode\yellow_tripdata_*.csv`

### 2.2 Raw Data Schema

The raw CSV files contain **19 columns** with the following structure:

```
┌──────────────────────────┬────────────┬─────────────────────────────────────────┐
│ Column Name              │ Data Type  │ Description                             │
├──────────────────────────┼────────────┼─────────────────────────────────────────┤
│ VendorID                 │ Integer    │ Taxi vendor identifier (1 or 2)         │
│ tpep_pickup_datetime     │ String     │ Pickup timestamp (YYYY-MM-DD HH:MM:SS)  │
│ tpep_dropoff_datetime    │ String     │ Dropoff timestamp                       │
│ passenger_count          │ Integer    │ Number of passengers                    │
│ trip_distance            │ Double     │ Trip distance in miles                  │
│ pickup_longitude         │ Double     │ Pickup GPS longitude                    │
│ pickup_latitude          │ Double     │ Pickup GPS latitude                     │
│ RatecodeID               │ Integer    │ Rate code for fare calculation          │
│ store_and_fwd_flag       │ String     │ Store and forward flag                  │
│ dropoff_longitude        │ Double     │ Dropoff GPS longitude                   │
│ dropoff_latitude         │ Double     │ Dropoff GPS latitude                    │
│ payment_type             │ Integer    │ Payment method                          │
│ fare_amount              │ Double     │ Base fare amount                        │
│ extra                    │ Double     │ Miscellaneous extras                    │
│ mta_tax                  │ Double     │ MTA tax                                 │
│ tip_amount               │ Double     │ Tip amount                              │
│ tolls_amount             │ Double     │ Tolls amount                            │
│ improvement_surcharge    │ Double     │ Improvement surcharge                   │
│ total_amount             │ Double     │ Total fare amount                       │
└──────────────────────────┴────────────┴─────────────────────────────────────────┘
```

### 2.3 Data Files Used

| File Name | Month | Purpose |
|-----------|-------|---------|
| `yellow_tripdata_2015-01.csv` | January 2015 | Historical data |
| `yellow_tripdata_2016-01.csv` | January 2016 | Training data |
| `yellow_tripdata_2016-02.csv` | February 2016 | Training data |
| `yellow_tripdata_2016-03.csv` | March 2016 | Test data |

### 2.4 NYC Geographic Bounds

The data is filtered to include only trips within NYC boundaries:

```
NYC Latitude Range:  40.4774 to 40.9176
NYC Longitude Range: -74.2591 to -73.7004
```

**Manhattan-specific bounds** (for is_manhattan flag):
```
Manhattan Latitude:  40.70 to 40.88
Manhattan Longitude: -74.02 to -73.93
```

---

## 3. System Architecture

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SMART CITY TRAFFIC SYSTEM                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────┐     ┌────────────────┐     ┌────────────────────────────┐  │
│  │ Raw CSV    │────▶│ Scala          │────▶│ Cleaned Parquet            │  │
│  │ Files      │     │ Preprocessor   │     │ Files                      │  │
│  │ (HDFS/     │     │                │     │ (HDFS/Local)               │  │
│  │ Local)     │     │ TrafficData    │     │                            │  │
│  └────────────┘     │ Preprocessor   │     │ *_clean.parquet            │  │
│                     │ .scala         │     └────────────────────────────┘  │
│                     └────────────────┘                │                    │
│                                                       ▼                    │
│                     ┌────────────────┐     ┌────────────────────────────┐  │
│                     │ PySpark        │────▶│ Feature Parquet            │  │
│                     │ Feature        │     │ Files                      │  │
│                     │ Engineering    │     │                            │  │
│                     │                │     │ training_features_spark    │  │
│                     │ feature_       │     │ .parquet                   │  │
│                     │ engineering_   │     └────────────────────────────┘  │
│                     │ spark.py       │                │                    │
│                     └────────────────┘                ▼                    │
│                                             ┌────────────────────────────┐  │
│                     ┌────────────────┐      │ Spark MLlib Model          │  │
│                     │ PySpark        │◀─────│                            │  │
│                     │ Model Training │      │ RandomForest               │  │
│                     │                │─────▶│ Classifier                 │  │
│                     │ model_         │      │                            │  │
│                     │ training_      │      │ spark_congestion_model     │  │
│                     │ spark.py       │      └────────────────────────────┘  │
│                     └────────────────┘                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Directory Structure

```
SmartCityTrafficSystem/
├── backend/
│   ├── src/
│   │   ├── scala/
│   │   │   ├── TrafficDataPreprocessor.scala    # Scala preprocessing
│   │   │   └── build.sbt                         # SBT build configuration
│   │   └── batch/
│   │       ├── data_cleaning_spark.py           # PySpark data cleaning
│   │       ├── feature_engineering_spark.py     # Feature engineering
│   │       ├── model_training_spark.py          # MLlib model training
│   │       └── hdfs_utils.py                    # HDFS utilities
│   ├── data/
│   │   └── processed/                           # Output Parquet files
│   └── models/
│       └── spark_congestion_model/              # Trained ML model
└── data/
    ├── raw/                                     # (symlink to raw data)
    └── processed/                               # Processed outputs
```

---

## 4. Data Pipeline Flow

### 4.1 Complete Data Flow Diagram

```
                            DATA TRANSFORMATION PIPELINE
                            ═══════════════════════════

    ┌─────────────────────────────────────────────────────────────────────┐
    │                          STAGE 1: RAW DATA                          │
    ├─────────────────────────────────────────────────────────────────────┤
    │  Input: yellow_tripdata_*.csv                                       │
    │  Format: CSV with 19 columns                                        │
    │  Records: ~12+ million per file                                     │
    │  Size: ~1.5-2 GB per file                                          │
    └───────────────────────────────────┬─────────────────────────────────┘
                                        │
                                        ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                    STAGE 2: SCALA PREPROCESSING                     │
    ├─────────────────────────────────────────────────────────────────────┤
    │  Script: TrafficDataPreprocessor.scala                              │
    │  Operations:                                                        │
    │    ✓ Load CSV with defined schema                                   │
    │    ✓ Standardize column names                                       │
    │    ✓ Convert datetime strings to timestamps                         │
    │    ✓ Filter invalid NYC coordinates                                 │
    │    ✓ Calculate duration_hours and speed_mph                         │
    │    ✓ Filter invalid trips (duration, distance, speed)               │
    │    ✓ Create spatial grid cells (1km x 1km)                         │
    │    ✓ Extract temporal features (hour, day, month, year)            │
    │    ✓ Create Manhattan flag                                         │
    │  Output: scala_cleaned.parquet                                      │
    └───────────────────────────────────┬─────────────────────────────────┘
                                        │
                                        ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                    STAGE 3: PYSPARK DATA CLEANING                   │
    ├─────────────────────────────────────────────────────────────────────┤
    │  Script: data_cleaning_spark.py                                     │
    │  Operations:                                                        │
    │    ✓ Same validations as Scala (alternative path)                   │
    │    ✓ Saves per-file cleaned Parquet                                │
    │  Output: yellow_tripdata_*_clean.parquet                           │
    └───────────────────────────────────┬─────────────────────────────────┘
                                        │
                                        ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                   STAGE 4: FEATURE ENGINEERING                      │
    ├─────────────────────────────────────────────────────────────────────┤
    │  Script: feature_engineering_spark.py                               │
    │  Operations:                                                        │
    │    ✓ Aggregate by cell_id + hour_bucket                            │
    │    ✓ Calculate trip_count, avg_speed, speed_std                    │
    │    ✓ Create congestion labels (Low/Medium/High)                    │
    │    ✓ Create LAGGED features (prev_trip_count, prev_avg_speed)      │
    │    ✓ Create historical averages                                    │
    │    ✓ Add temporal features (is_weekend, is_rush_hour, is_night)    │
    │    ✓ Add train/test split column (temporal)                        │
    │  Output: training_features_spark.parquet                           │
    │  Columns: 17 features + target + metadata                          │
    └───────────────────────────────────┬─────────────────────────────────┘
                                        │
                                        ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                    STAGE 5: MODEL TRAINING                          │
    ├─────────────────────────────────────────────────────────────────────┤
    │  Script: model_training_spark.py                                    │
    │  Operations:                                                        │
    │    ✓ Load training features                                        │
    │    ✓ Split: Jan-Feb = Train, March = Test (Temporal)               │
    │    ✓ VectorAssembler → StandardScaler → RandomForestClassifier     │
    │    ✓ Train with 100 trees, maxDepth=10                            │
    │    ✓ Evaluate: accuracy, precision, recall, F1                     │
    │    ✓ Extract feature importance                                    │
    │  Output: spark_congestion_model/                                   │
    │  Expected Accuracy: 70-85% (realistic, no data leakage)           │
    └─────────────────────────────────────────────────────────────────────┘
```

### 4.2 Schema Transformation

```
RAW CSV (19 columns)
        │
        ▼
CLEANED PARQUET (20 columns)
├── pickup_datetime (timestamp)
├── dropoff_datetime (timestamp)
├── pickup_lat (double)
├── pickup_lon (double)
├── dropoff_lat (double)
├── dropoff_lon (double)
├── trip_distance (double)
├── duration_hours (double)      ← DERIVED
├── speed_mph (double)           ← DERIVED
├── passenger_count (integer)
├── fare_amount (double)
├── total_amount (double)
├── cell_id (string)             ← DERIVED (spatial)
├── cell_lat (integer)           ← DERIVED (spatial)
├── cell_lon (integer)           ← DERIVED (spatial)
├── hour (integer)               ← DERIVED (temporal)
├── day_of_week (integer)        ← DERIVED (temporal)
├── month (integer)              ← DERIVED (temporal)
├── year (integer)               ← DERIVED (temporal)
└── is_manhattan (boolean)       ← DERIVED (spatial)
        │
        ▼
FEATURE PARQUET (23+ columns)
├── hour (integer)
├── day_of_week (integer)
├── month (integer)
├── is_weekend (integer)         ← DERIVED
├── is_rush_hour (integer)       ← DERIVED
├── is_night (integer)           ← DERIVED
├── cell_lat (integer)
├── cell_lon (integer)
├── is_manhattan_int (integer)
├── prev_trip_count (double)     ← LAGGED (1 hour)
├── prev_avg_speed (double)      ← LAGGED (1 hour)
├── prev_congestion_label (int)  ← LAGGED (1 hour)
├── prev_2h_trip_count (double)  ← LAGGED (2 hours)
├── prev_2h_avg_speed (double)   ← LAGGED (2 hours)
├── historical_avg_trips (double) ← AGGREGATED
├── historical_avg_speed (double) ← AGGREGATED
├── congestion_label (integer)   ← TARGET (0/1/2)
├── congestion_level (string)    ← TARGET (Low/Medium/High)
├── avg_speed (double)           ← For analysis only (NOT in features!)
├── cell_id (string)             ← Metadata
├── hour_bucket (timestamp)      ← Metadata
├── year (integer)               ← Metadata
└── dataset_split (string)       ← train/test indicator
```

---

## 5. Scala Preprocessing Module

### 5.1 File: `TrafficDataPreprocessor.scala`

**Location**: `backend/src/scala/TrafficDataPreprocessor.scala`  
**Lines**: 406  
**Purpose**: High-performance data preprocessing using Scala and Spark SQL

### 5.2 Module Structure

```scala
object TrafficDataPreprocessor {
  
  // Constants
  val NYC_LAT_MIN = 40.4774
  val NYC_LAT_MAX = 40.9176
  val NYC_LON_MIN = -74.2591
  val NYC_LON_MAX = -73.7004
  val CELL_SIZE = 0.01  // ~1km x 1km
  
  // Core Functions
  def createSparkSession(): SparkSession
  def getTaxiSchema(): StructType
  def loadRawData(spark, inputPath): DataFrame
  def standardizeColumns(df): DataFrame
  def convertDatetimes(df): DataFrame
  def filterValidCoordinates(df): DataFrame
  def calculateDerivedMetrics(df): DataFrame
  def filterValidTrips(df): DataFrame
  def createGridCells(df): DataFrame
  def extractTemporalFeatures(df): DataFrame
  def createManhattanFlag(df): DataFrame
  def selectFinalColumns(df): DataFrame
  def printStatistics(df, label): Unit
  def saveToParquet(df, outputPath): Unit
  def preprocessData(spark, inputPath, outputPath): DataFrame
  def aggregateByCellHour(df): DataFrame
  def createCongestionLabels(df): DataFrame
  def main(args: Array[String]): Unit
}
```

### 5.3 Key Functions Explained

#### 5.3.1 Schema Definition

```scala
def getTaxiSchema(): StructType = {
  StructType(Array(
    StructField("VendorID", IntegerType, true),
    StructField("tpep_pickup_datetime", StringType, true),
    StructField("tpep_dropoff_datetime", StringType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("RatecodeID", IntegerType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true)
  ))
}
```

**Why Explicit Schema?**
- **Performance**: Avoids schema inference which requires an extra pass over data
- **Type Safety**: Ensures correct data types for downstream processing
- **Error Handling**: DROPMALFORMED mode skips corrupt rows

#### 5.3.2 Coordinate Filtering

```scala
def filterValidCoordinates(df: DataFrame): DataFrame = {
  df.filter(
    col("pickup_lat").between(NYC_LAT_MIN, NYC_LAT_MAX) &&
    col("pickup_lon").between(NYC_LON_MIN, NYC_LON_MAX) &&
    col("dropoff_lat").between(NYC_LAT_MIN, NYC_LAT_MAX) &&
    col("dropoff_lon").between(NYC_LON_MIN, NYC_LON_MAX)
  )
}
```

**Purpose**: Removes trips with:
- Coordinates outside NYC bounds
- GPS errors (0, 0 coordinates)
- Invalid latitude/longitude values

#### 5.3.3 Derived Metrics Calculation

```scala
def calculateDerivedMetrics(df: DataFrame): DataFrame = {
  df.withColumn("duration_seconds",
      unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))
    .withColumn("duration_hours", col("duration_seconds") / 3600.0)
    .withColumn("speed_mph", 
      round(col("trip_distance") / col("duration_hours"), 2))
}
```

**Key Insights**:
- `duration_seconds` = dropoff_time - pickup_time
- `duration_hours` = duration_seconds / 3600
- `speed_mph` = trip_distance / duration_hours

#### 5.3.4 Trip Validation Filters

```scala
def filterValidTrips(df: DataFrame): DataFrame = {
  df.filter(
    // Duration between 1 minute and 3 hours
    col("duration_seconds").between(60, 10800) &&
    // Distance between 0.1 and 100 miles
    col("trip_distance").between(0.1, 100) &&
    // Speed between 1 and 60 mph
    col("speed_mph").between(1, 60)
  )
}
```

**Filter Thresholds**:

| Metric | Minimum | Maximum | Reason |
|--------|---------|---------|--------|
| Duration | 60 sec | 10,800 sec (3 hrs) | Remove testing trips and stuck meters |
| Distance | 0.1 miles | 100 miles | Remove zero-distance and cross-state trips |
| Speed | 1 mph | 60 mph | Remove stationary vehicles and racing/errors |

#### 5.3.5 Spatial Grid Cell Creation

```scala
def createGridCells(df: DataFrame): DataFrame = {
  df.withColumn("cell_lat",
      ((col("pickup_lat") - lit(NYC_LAT_MIN)) / lit(CELL_SIZE)).cast(IntegerType))
    .withColumn("cell_lon",
      ((col("pickup_lon") - lit(NYC_LON_MIN)) / lit(CELL_SIZE)).cast(IntegerType))
    .withColumn("cell_id",
      concat_ws("_", lit("cell"), col("cell_lat"), col("cell_lon")))
}
```

**Grid Cell Logic**:

```
CELL_SIZE = 0.01 degrees ≈ 1.11 km (at NYC latitude)

cell_lat = floor((pickup_lat - 40.4774) / 0.01)
cell_lon = floor((pickup_lon - (-74.2591)) / 0.01)
cell_id = "cell_" + cell_lat + "_" + cell_lon

Example:
  Coordinates: (40.7580, -73.9855)
  cell_lat = floor((40.7580 - 40.4774) / 0.01) = floor(28.06) = 28
  cell_lon = floor((-73.9855 - (-74.2591)) / 0.01) = floor(27.36) = 27
  cell_id = "cell_28_27"
```

**Grid Coverage**:
- Latitude cells: ~44 cells (40.9176 - 40.4774) / 0.01
- Longitude cells: ~56 cells (74.2591 - 73.7004) / 0.01
- Total possible cells: ~2,464

#### 5.3.6 Congestion Labels

```scala
def createCongestionLabels(df: DataFrame): DataFrame = {
  df.withColumn("congestion_label",
      when(col("avg_speed") > 20, 0)      // Low
      .when(col("avg_speed") >= 10, 1)    // Medium
      .otherwise(2))                       // High
    .withColumn("congestion_level",
      when(col("congestion_label") === 0, "Low")
      .when(col("congestion_label") === 1, "Medium")
      .otherwise("High"))
}
```

**Congestion Thresholds**:

| Label | Level | Speed Range | Interpretation |
|-------|-------|-------------|----------------|
| 0 | Low | > 20 mph | Free-flowing traffic |
| 1 | Medium | 10-20 mph | Moderate congestion |
| 2 | High | < 10 mph | Severe congestion |

### 5.4 Build Configuration

**File**: `build.sbt`

```scala
name := "traffic-preprocessor"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.5.0" % "provided"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```

**Running the Scala Preprocessor**:

```bash
# Option 1: Spark Submit
spark-submit --class TrafficDataPreprocessor target/scala-2.12/traffic-preprocessor.jar

# Option 2: Spark Shell
:load src/scala/TrafficDataPreprocessor.scala
TrafficDataPreprocessor.main(Array())
```

---

## 6. PySpark Data Cleaning

### 6.1 File: `data_cleaning_spark.py`

**Location**: `backend/src/batch/data_cleaning_spark.py`  
**Lines**: 426  
**Purpose**: Alternative data cleaning using PySpark (same logic as Scala)

### 6.2 Spark Session Configuration

```python
def create_spark_session(use_hdfs=False):
    # Windows workaround for Hadoop
    if os.name == 'nt':
        hadoop_home = r"C:\hadoop"
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ['PATH'] += f";{hadoop_home}\\bin"
    
    builder = SparkSession.builder \
        .appName("SmartCityTraffic-DataCleaning") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "20")
    
    if use_hdfs:
        builder = builder \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    
    return builder.master("local[*]").getOrCreate()
```

**Key Configurations**:

| Config | Value | Purpose |
|--------|-------|---------|
| `spark.driver.memory` | 2g | Driver JVM heap size |
| `spark.executor.memory` | 2g | Executor JVM heap size |
| `spark.sql.parquet.compression.codec` | snappy | Fast compression |
| `spark.sql.shuffle.partitions` | 20 | Parallelism for shuffles |
| `spark.hadoop.fs.defaultFS` | hdfs://localhost:9000 | HDFS namenode |

### 6.3 Transformation Pipeline

```python
def clean_and_transform(df):
    # Step 1: Rename columns
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
           .withColumnRenamed("pickup_longitude", "pickup_lon") \
           .withColumnRenamed("pickup_latitude", "pickup_lat") \
           .withColumnRenamed("dropoff_longitude", "dropoff_lon") \
           .withColumnRenamed("dropoff_latitude", "dropoff_lat")
    
    # Step 2: Convert datetime strings to timestamps
    df = df.withColumn("pickup_datetime", 
                       to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
    
    # Step 3: Filter NYC coordinates
    df = df.filter(
        (col("pickup_lat").between(40.4774, 40.9176)) &
        (col("pickup_lon").between(-74.2591, -73.7004)) &
        (col("dropoff_lat").between(40.4774, 40.9176)) &
        (col("dropoff_lon").between(-74.2591, -73.7004))
    )
    
    # Step 4-5: Calculate duration
    df = df.withColumn("duration_seconds",
        unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))
    df = df.filter(col("duration_seconds").between(60, 10800))
    
    # Step 6: Filter distance
    df = df.filter(col("trip_distance").between(0.1, 100))
    
    # Step 7-8: Calculate and filter speed
    df = df.withColumn("speed_mph",
        spark_round(col("trip_distance") / (col("duration_seconds") / 3600.0), 2))
    df = df.filter(col("speed_mph").between(1, 60))
    
    # Step 9: Create grid cells
    df = df.withColumn("cell_lat",
        ((col("pickup_lat") - lit(40.4774)) / lit(0.01)).cast(IntegerType()))
    df = df.withColumn("cell_lon",
        ((col("pickup_lon") - lit(-74.2591)) / lit(0.01)).cast(IntegerType()))
    df = df.withColumn("cell_id",
        concat_ws("_", lit("cell"), col("cell_lat"), col("cell_lon")))
    
    # Step 10: Temporal features
    df = df.withColumn("hour", hour(col("pickup_datetime"))) \
           .withColumn("day_of_week", dayofweek(col("pickup_datetime"))) \
           .withColumn("month", month(col("pickup_datetime"))) \
           .withColumn("year", year(col("pickup_datetime")))
    
    # Step 11: Manhattan flag
    df = df.withColumn("is_manhattan",
        when((col("pickup_lat").between(40.70, 40.88)) &
             (col("pickup_lon").between(-74.02, -73.93)), True)
        .otherwise(False))
    
    return df
```

### 6.4 Output Files

The data cleaning script produces per-file Parquet outputs:

```
data/processed/
├── yellow_tripdata_2015-01_clean.parquet/
├── yellow_tripdata_2016-01_clean.parquet/
├── yellow_tripdata_2016-02_clean.parquet/
└── yellow_tripdata_2016-03_clean.parquet/
```

---

## 7. Feature Engineering with Spark

### 7.1 File: `feature_engineering_spark.py`

**Location**: `backend/src/batch/feature_engineering_spark.py`  
**Lines**: 523  
**Purpose**: Create ML-ready features with proper lagging to prevent data leakage

### 7.2 Aggregation by Cell and Hour

```python
def aggregate_by_cell_hour(df):
    # Create hour bucket
    df = df.withColumn("hour_bucket", date_trunc("hour", col("pickup_datetime")))
    
    # Aggregate metrics per cell-hour
    agg_df = df.groupBy(
        "cell_id", "cell_lat", "cell_lon", "hour_bucket", 
        "hour", "day_of_week", "month", "year"
    ).agg(
        count("*").alias("trip_count"),
        avg("speed_mph").alias("avg_speed"),
        stddev("speed_mph").alias("speed_std"),
        spark_min("speed_mph").alias("min_speed"),
        spark_max("speed_mph").alias("max_speed"),
        avg("trip_distance").alias("avg_distance"),
        avg("duration_hours").alias("avg_duration"),
        first("is_manhattan").alias("is_manhattan")
    )
    
    # Fill null std (when only 1 trip)
    agg_df = agg_df.withColumn("speed_std", coalesce(col("speed_std"), lit(0.0)))
    
    return agg_df
```

**Result**: One row per (cell_id, hour_bucket) combination

### 7.3 Lagged Features (Critical for No Data Leakage)

```python
def create_lagged_features(df):
    # Window: partition by cell, order by time
    cell_time_window = Window.partitionBy("cell_lat", "cell_lon").orderBy("hour_bucket")
    
    # 1-hour lag features
    df = df.withColumn("prev_trip_count", lag("trip_count", 1).over(cell_time_window))
    df = df.withColumn("prev_avg_speed", lag("avg_speed", 1).over(cell_time_window))
    df = df.withColumn("prev_congestion_label", lag("congestion_label", 1).over(cell_time_window))
    
    # 2-hour lag features
    df = df.withColumn("prev_2h_trip_count", lag("trip_count", 2).over(cell_time_window))
    df = df.withColumn("prev_2h_avg_speed", lag("avg_speed", 2).over(cell_time_window))
    
    return df
```

**Why Lagged Features?**

```
Without Lagging (DATA LEAKAGE):
  Features at time T include avg_speed at time T
  → Model learns: low avg_speed = high congestion
  → Model achieves ~100% accuracy (but useless for prediction!)

With Lagging (PROPER ML):
  Features at time T use data from time T-1 and T-2
  → Model must predict future from past
  → Model achieves 70-85% accuracy (realistic and useful!)
```

### 7.4 Temporal Features

```python
def create_temporal_features(df):
    # Is weekend (Saturday=7, Sunday=1)
    df = df.withColumn("is_weekend",
        when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
    
    # Is rush hour (7-9 AM or 5-7 PM)
    df = df.withColumn("is_rush_hour",
        when(
            (col("hour").between(7, 9)) | (col("hour").between(17, 19)),
            1
        ).otherwise(0))
    
    # Is night (10 PM - 6 AM)
    df = df.withColumn("is_night",
        when(
            (col("hour") >= 22) | (col("hour") <= 6),
            1
        ).otherwise(0))
    
    return df
```

### 7.5 Final Feature Set (17 Features)

```python
feature_columns = [
    # Temporal features (6)
    "hour",
    "day_of_week",
    "month",
    "is_weekend",
    "is_rush_hour",
    "is_night",
    
    # Spatial features (3)
    "cell_lat",
    "cell_lon",
    "is_manhattan_int",
    
    # Lagged features (5)
    "prev_trip_count",
    "prev_avg_speed",
    "prev_congestion_label",
    "prev_2h_trip_count",
    "prev_2h_avg_speed",
    
    # Historical features (2)
    "historical_avg_trips",
    "historical_avg_speed"
]

# NOTE: avg_speed is NOT included! (prevents data leakage)
```

### 7.6 Train/Test Split

```python
def add_train_test_split_column(df):
    df = df.withColumn("dataset_split",
        when(col("month").isin([1, 2]), "train")   # Jan-Feb = Training
        .when(col("month") == 3, "test")            # March = Testing
        .otherwise("other")
    )
    return df
```

**Temporal Split Rationale**:
- Training on Jan-Feb data
- Testing on March data
- Simulates real-world scenario: predicting future from historical patterns

---

## 8. Model Training with Spark MLlib

### 8.1 File: `model_training_spark.py`

**Location**: `backend/src/batch/model_training_spark.py`  
**Lines**: 513  
**Purpose**: Train RandomForest classifier using Spark MLlib

### 8.2 ML Pipeline Architecture

```python
def create_ml_pipeline(feature_columns):
    # Step 1: Combine features into vector
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    
    # Step 2: Normalize features
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    # Step 3: Random Forest Classifier
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="congestion_label",
        predictionCol="prediction",
        probabilityCol="probability",
        numTrees=100,
        maxDepth=10,
        minInstancesPerNode=10,
        featureSubsetStrategy="sqrt",
        seed=42
    )
    
    # Create pipeline
    return Pipeline(stages=[assembler, scaler, rf])
```

**Pipeline Visualization**:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐
│ VectorAssembler │───▶│ StandardScaler  │───▶│ RandomForestClassifier  │
├─────────────────┤    ├─────────────────┤    ├─────────────────────────┤
│ 17 feature cols │    │ features_raw    │    │ features (scaled)       │
│    ▼            │    │      ▼          │    │      ▼                  │
│ features_raw    │    │ features        │    │ prediction              │
│ (DenseVector)   │    │ (DenseVector)   │    │ probability             │
│                 │    │ (normalized)    │    │ rawPrediction           │
└─────────────────┘    └─────────────────┘    └─────────────────────────┘
```

### 8.3 Model Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `numTrees` | 100 | Number of trees in the forest |
| `maxDepth` | 10 | Maximum depth of each tree |
| `minInstancesPerNode` | 10 | Minimum samples per leaf |
| `featureSubsetStrategy` | "sqrt" | Features considered per split |
| `seed` | 42 | Random seed for reproducibility |

### 8.4 Model Evaluation

```python
def evaluate_model(model, test_df, train_df):
    test_predictions = model.transform(test_df)
    train_predictions = model.transform(train_df)
    
    # Evaluators
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="congestion_label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    metrics = {
        "train_accuracy": accuracy_evaluator.evaluate(train_predictions),
        "test_accuracy": accuracy_evaluator.evaluate(test_predictions),
        "test_precision": precision_evaluator.evaluate(test_predictions),
        "test_recall": recall_evaluator.evaluate(test_predictions),
        "test_f1": f1_evaluator.evaluate(test_predictions)
    }
    
    return metrics
```

### 8.5 Expected Results

| Metric | Training | Test | Notes |
|--------|----------|------|-------|
| Accuracy | 80-90% | 70-85% | Realistic performance |
| Precision | - | 70-80% | Weighted average |
| Recall | - | 70-80% | Weighted average |
| F1-Score | - | 70-80% | Harmonic mean |

**Why Not 100% Accuracy?**
- `avg_speed` is NOT in features (prevents trivial mapping)
- Temporal train/test split (prediction, not interpolation)
- Real-world traffic is inherently unpredictable

### 8.6 Feature Importance

```python
def get_feature_importance(model, feature_columns):
    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray()
    
    feature_importance = list(zip(feature_columns, importances))
    feature_importance.sort(key=lambda x: x[1], reverse=True)
    
    return dict(feature_importance)
```

**Typical Top Features**:
1. `prev_avg_speed` - Previous hour's speed (most predictive)
2. `prev_congestion_label` - Previous congestion state
3. `hour` - Time of day
4. `historical_avg_speed` - Expected speed for this cell/hour
5. `is_manhattan_int` - Manhattan has unique patterns

### 8.7 Model Output

```
models/
├── spark_congestion_model/          # Spark MLlib model directory
│   ├── metadata/
│   │   └── part-00000
│   └── stages/
│       ├── 0_vectorAssembler/
│       ├── 1_standardScaler/
│       └── 2_randomForest/
├── model_info_spark.json            # Model metadata
└── feature_columns_spark.json       # Feature column names
```

---

## 9. HDFS Integration

### 9.1 File: `hdfs_utils.py`

**Location**: `backend/src/batch/hdfs_utils.py`  
**Lines**: 342  
**Purpose**: Utilities for HDFS data management

### 9.2 HDFS Directory Structure

```
hdfs://localhost:9000/
└── smart-city-traffic/
    └── data/
        ├── raw/                    # Raw CSV files
        │   ├── yellow_tripdata_2016-01/
        │   ├── yellow_tripdata_2016-02/
        │   └── yellow_tripdata_2016-03/
        ├── processed/              # Cleaned Parquet files
        │   ├── yellow_tripdata_2016-01_clean.parquet/
        │   ├── yellow_tripdata_2016-02_clean.parquet/
        │   └── yellow_tripdata_2016-03_clean.parquet/
        ├── features/               # Feature Parquet files
        │   └── training_features_spark.parquet/
        └── models/                 # Trained models
            └── spark_congestion_model/
```

### 9.3 Key HDFS Operations

```python
# Upload data to HDFS
python src/batch/hdfs_utils.py upload

# Upload using Spark (better for large files)
python src/batch/hdfs_utils.py upload-spark

# List HDFS contents
python src/batch/hdfs_utils.py list

# Check HDFS health
python src/batch/hdfs_utils.py health

# Create directory structure
python src/batch/hdfs_utils.py setup
```

### 9.4 Running with HDFS Mode

```bash
# Data Cleaning (HDFS mode)
python src/batch/data_cleaning_spark.py --hdfs

# Feature Engineering (HDFS mode)
python src/batch/feature_engineering_spark.py --hdfs

# Model Training (HDFS mode)
python src/batch/model_training_spark.py --hdfs
```

---

## 10. Output Artifacts

### 10.1 Processed Data Directory

```
data/processed/
├── feature_columns_spark.json                    # 322 bytes
├── training_features.parquet                     # 236 KB
├── training_features_sample.csv                  # 99 KB
├── training_features_spark.parquet/              # Directory (Spark)
├── yellow_tripdata_2015-01_clean.parquet/        # Directory (Spark)
├── yellow_tripdata_2016-01_clean.parquet/        # Directory (Spark)
├── yellow_tripdata_2016-02_clean.parquet/        # Directory (Spark)
└── yellow_tripdata_2016-03_clean.parquet/        # Directory (Spark)
```

### 10.2 Parquet Format Benefits

| Benefit | Description |
|---------|-------------|
| **Columnar Storage** | Efficient for analytics queries |
| **Compression** | Snappy codec reduces size by 5-10x |
| **Schema Preservation** | Types maintained automatically |
| **Predicate Pushdown** | Filters applied during read |
| **Spark Native** | Optimal for Spark processing |

### 10.3 Model Artifacts

```
models/
├── spark_congestion_model/          # Spark MLlib model
│   ├── metadata/
│   └── stages/
├── model_info_spark.json            # Training metrics & metadata
└── feature_columns_spark.json       # Feature column names
```

**Sample `model_info_spark.json`**:

```json
{
  "model_type": "Spark MLlib RandomForestClassifier",
  "trained_at": "2026-01-18T19:00:00",
  "spark_model_path": "models/spark_congestion_model",
  "features": [
    "hour", "day_of_week", "month", "is_weekend",
    "is_rush_hour", "is_night", "cell_lat", "cell_lon",
    "is_manhattan_int", "prev_trip_count", "prev_avg_speed",
    "prev_congestion_label", "prev_2h_trip_count",
    "prev_2h_avg_speed", "historical_avg_trips", "historical_avg_speed"
  ],
  "metrics": {
    "train_accuracy": 0.85,
    "test_accuracy": 0.78,
    "test_precision": 0.76,
    "test_recall": 0.78,
    "test_f1": 0.77
  },
  "classes": ["Low", "Medium", "High"],
  "thresholds": {
    "low": "> 20 mph",
    "medium": "10-20 mph",
    "high": "< 10 mph"
  },
  "notes": [
    "Model uses Spark MLlib RandomForestClassifier",
    "avg_speed NOT included in features (prevents data leakage)",
    "Temporal train/test split (Jan-Feb train, March test)",
    "Uses lagged features for true prediction"
  ]
}
```

---

## Quick Commands Reference

### Full Pipeline Execution

```bash
# 1. Data Cleaning (Local)
python backend/src/batch/data_cleaning_spark.py

# 2. Feature Engineering (Local)
python backend/src/batch/feature_engineering_spark.py

# 3. Model Training (Local)
python backend/src/batch/model_training_spark.py

# --- OR with HDFS ---

# 1. Setup HDFS directories
python backend/src/batch/hdfs_utils.py setup

# 2. Upload data to HDFS
python backend/src/batch/hdfs_utils.py upload-spark

# 3. Data Cleaning (HDFS)
python backend/src/batch/data_cleaning_spark.py --hdfs

# 4. Feature Engineering (HDFS)
python backend/src/batch/feature_engineering_spark.py --hdfs

# 5. Model Training (HDFS)
python backend/src/batch/model_training_spark.py --hdfs
```

### Scala Preprocessing

```bash
# Compile (from backend/src/scala/)
sbt package

# Run
spark-submit --class TrafficDataPreprocessor target/scala-2.12/traffic-preprocessor.jar
```

---

## Summary

This Smart City Traffic System implements a complete Big Data preprocessing pipeline:

1. **Raw Data**: NYC Yellow Taxi CSV files (~2GB each)
2. **Scala Preprocessing**: High-performance cleaning with explicit schema
3. **PySpark Cleaning**: Alternative cleaning path with identical logic
4. **Feature Engineering**: Aggregation, lagging, and temporal features
5. **Spark MLlib Training**: RandomForest classifier with proper ML practices
6. **HDFS Integration**: Distributed storage for scalability

**Key Design Decisions**:
- ✅ Explicit schema for performance
- ✅ Lagged features to prevent data leakage
- ✅ Temporal train/test split
- ✅ `avg_speed` excluded from features
- ✅ Parquet format for efficient storage
- ✅ HDFS support for distributed processing

---

*Document generated: January 18, 2026*  
*Project: Smart City Traffic Congestion Prediction System*
