#!/usr/bin/env python3
"""
Smart City Traffic - Docker Data Cleaning Script
=================================================
This is a standalone version of data_cleaning_spark.py designed to run
inside the Docker container. It has minimal dependencies and hardcoded
paths for the Docker environment.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, unix_timestamp, round as spark_round,
    hour, dayofweek, month, year, to_timestamp,
    lit, count, avg, min as spark_min, max as spark_max, concat_ws
)
from pyspark.sql.types import IntegerType

# =============================================================================
# CONFIGURATION (Hardcoded for Docker)
# =============================================================================
HDFS_NAMENODE = "hdfs://namenode:9000"
HDFS_RAW_DIR = "/smart-city-traffic/data/raw"
HDFS_PROCESSED_DIR = "/smart-city-traffic/data/processed"

# NYC Geographic bounds
NYC_LAT_MIN = 40.4774
NYC_LAT_MAX = 40.9176
NYC_LON_MIN = -74.2591
NYC_LON_MAX = -73.7004
CELL_SIZE = 0.01


def create_spark_session():
    """Create Spark session for cluster mode."""
    spark = SparkSession.builder \
        .appName("SmartCityTraffic-DataCleaning-Docker") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_taxi_files_hdfs(spark):
    """Find all taxi CSV files in HDFS."""
    print(f"\nLooking for taxi data files in HDFS: {HDFS_RAW_DIR}")
    print(f"  Using namenode: {HDFS_NAMENODE}")
    
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI(HDFS_NAMENODE),
        hadoop_conf
    )
    
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(HDFS_RAW_DIR)
    
    files = []
    try:
        file_statuses = fs.listStatus(path)
        for status in file_statuses:
            file_path = str(status.getPath())
            if "yellow_tripdata" in file_path:
                files.append(file_path)
                print(f"  - {file_path}")
    except Exception as e:
        print(f"  Error listing HDFS: {e}")
    
    print(f"\nFound {len(files)} taxi data files in HDFS")
    return files


def load_raw_data(spark, file_path):
    """Load raw CSV data into Spark DataFrame."""
    print(f"\nLoading: {file_path}")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "DROPMALFORMED") \
        .csv(file_path)
    
    initial_count = df.count()
    print(f"  Loaded {initial_count:,} rows")
    return df


def clean_and_transform(df):
    """Apply cleaning transformations."""
    print("\nApplying cleaning transformations...")
    
    # Rename columns
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
           .withColumnRenamed("pickup_longitude", "pickup_lon") \
           .withColumnRenamed("pickup_latitude", "pickup_lat") \
           .withColumnRenamed("dropoff_longitude", "dropoff_lon") \
           .withColumnRenamed("dropoff_latitude", "dropoff_lat")
    
    # Convert datetime
    df = df.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
           .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
    
    # Filter coordinates
    df = df.filter(
        (col("pickup_lat").between(NYC_LAT_MIN, NYC_LAT_MAX)) &
        (col("pickup_lon").between(NYC_LON_MIN, NYC_LON_MAX)) &
        (col("dropoff_lat").between(NYC_LAT_MIN, NYC_LAT_MAX)) &
        (col("dropoff_lon").between(NYC_LON_MIN, NYC_LON_MAX))
    )
    print(f"  After coordinate filter: {df.count():,} rows")
    
    # Calculate duration
    df = df.withColumn(
        "duration_seconds",
        unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))
    )
    df = df.withColumn("duration_hours", col("duration_seconds") / 3600.0)
    
    # Filter duration
    df = df.filter((col("duration_seconds") >= 60) & (col("duration_seconds") <= 10800))
    print(f"  After duration filter: {df.count():,} rows")
    
    # Filter distance
    df = df.filter((col("trip_distance") >= 0.1) & (col("trip_distance") <= 100))
    print(f"  After distance filter: {df.count():,} rows")
    
    # Calculate speed
    df = df.withColumn("speed_mph", spark_round(col("trip_distance") / col("duration_hours"), 2))
    
    # Filter speed
    df = df.filter((col("speed_mph") >= 1) & (col("speed_mph") <= 60))
    print(f"  After speed filter: {df.count():,} rows")
    
    # Create grid cells
    df = df.withColumn("cell_lat", ((col("pickup_lat") - lit(NYC_LAT_MIN)) / lit(CELL_SIZE)).cast(IntegerType()))
    df = df.withColumn("cell_lon", ((col("pickup_lon") - lit(NYC_LON_MIN)) / lit(CELL_SIZE)).cast(IntegerType()))
    df = df.withColumn("cell_id", concat_ws("_", lit("cell"), col("cell_lat"), col("cell_lon")))
    
    # Temporal features
    df = df.withColumn("hour", hour(col("pickup_datetime"))) \
           .withColumn("day_of_week", dayofweek(col("pickup_datetime"))) \
           .withColumn("month", month(col("pickup_datetime"))) \
           .withColumn("year", year(col("pickup_datetime")))
    
    # Manhattan flag
    df = df.withColumn(
        "is_manhattan",
        when((col("pickup_lat").between(40.70, 40.88)) & (col("pickup_lon").between(-74.02, -73.93)), True).otherwise(False)
    )
    
    # Select final columns
    final_columns = [
        "pickup_datetime", "dropoff_datetime",
        "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
        "trip_distance", "duration_hours", "speed_mph",
        "passenger_count", "fare_amount", "total_amount",
        "cell_id", "cell_lat", "cell_lon",
        "hour", "day_of_week", "month", "year", "is_manhattan"
    ]
    
    return df.select(final_columns)


def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("SMART CITY TRAFFIC - DOCKER DATA CLEANING")
    print("=" * 60)
    print("Running in Docker Cluster Mode")
    print(f"HDFS Namenode: {HDFS_NAMENODE}")
    
    start_time = datetime.now()
    spark = create_spark_session()
    
    taxi_files = get_taxi_files_hdfs(spark)
    
    if not taxi_files:
        print("ERROR: No taxi data files found!")
        spark.stop()
        return
    
    total_records = 0
    for file_path in taxi_files:
        print(f"\n{'=' * 60}")
        file_name = file_path.split("/")[-1]
        print(f"Processing: {file_name}")
        print("=" * 60)
        
        df = load_raw_data(spark, file_path)
        df_clean = clean_and_transform(df)
        
        base_name = file_name.replace(".csv", "")
        output_path = f"{HDFS_NAMENODE}{HDFS_PROCESSED_DIR}/{base_name}_clean.parquet"
        
        print(f"\nSaving to: {output_path}")
        df_clean.write.mode("overwrite").parquet(output_path)
        print("  ✓ Saved successfully!")
        
        total_records += df_clean.count()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("CLEANING COMPLETE")
    print("=" * 60)
    print(f"  Files processed: {len(taxi_files)}")
    print(f"  Total records: {total_records:,}")
    print(f"  Duration: {duration:.1f} seconds")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
