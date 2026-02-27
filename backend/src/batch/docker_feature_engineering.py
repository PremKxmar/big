#!/usr/bin/env python3
"""
Smart City Traffic - Docker Feature Engineering Script
========================================================
Standalone version for Docker container execution.
"""

import os
import sys
from datetime import datetime
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, avg, stddev, min as spark_min, max as spark_max,
    hour, dayofweek, month, date_trunc, lag, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# =============================================================================
# CONFIGURATION (Hardcoded for Docker)
# =============================================================================
HDFS_NAMENODE = "hdfs://namenode:9000"
HDFS_PROCESSED_DIR = "/smart-city-traffic/data/processed"
HDFS_FEATURES_DIR = "/smart-city-traffic/data/features"


def create_spark_session():
    """Create Spark session for cluster mode."""
    spark = SparkSession.builder \
        .appName("SmartCityTraffic-FeatureEngineering-Docker") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_cleaned_data(spark):
    """Load all cleaned Parquet files."""
    parquet_path = f"{HDFS_NAMENODE}{HDFS_PROCESSED_DIR}/*_clean.parquet"
    print(f"\nLoading cleaned data from: {parquet_path}")
    
    df = spark.read.parquet(parquet_path)
    print(f"  ✓ Loaded {df.count():,} records")
    return df


def aggregate_by_cell_hour(df):
    """Aggregate trip data by cell and hour."""
    print("\nAggregating by Cell and Hour...")
    
    from pyspark.sql.functions import first
    
    df = df.withColumn("hour_bucket", date_trunc("hour", col("pickup_datetime")))
    
    agg_df = df.groupBy("cell_id", "cell_lat", "cell_lon", "hour_bucket", "hour", "day_of_week", "month", "year") \
        .agg(
            count("*").alias("trip_count"),
            avg("speed_mph").alias("avg_speed"),
            stddev("speed_mph").alias("speed_std"),
            spark_min("speed_mph").alias("min_speed"),
            spark_max("speed_mph").alias("max_speed"),
            avg("trip_distance").alias("avg_distance"),
            avg("duration_hours").alias("avg_duration"),
            first("is_manhattan").alias("is_manhattan")
        )
    
    agg_df = agg_df.withColumn("speed_std", coalesce(col("speed_std"), lit(0.0)))
    print(f"  Aggregated records: {agg_df.count():,}")
    return agg_df


def create_congestion_labels(df):
    """Create congestion labels based on speed."""
    print("\nCreating Congestion Labels...")
    
    df = df.withColumn(
        "congestion_label",
        when(col("avg_speed") > 20, 0)
        .when(col("avg_speed") >= 10, 1)
        .otherwise(2)
    )
    
    df = df.withColumn(
        "congestion_level",
        when(col("congestion_label") == 0, "Low")
        .when(col("congestion_label") == 1, "Medium")
        .otherwise("High")
    )
    return df


def create_lagged_features(df):
    """Create lagged features using Window functions."""
    print("\nCreating Lagged Features...")
    
    cell_time_window = Window.partitionBy("cell_lat", "cell_lon").orderBy("hour_bucket")
    
    df = df.withColumn("prev_trip_count", lag("trip_count", 1).over(cell_time_window))
    df = df.withColumn("prev_avg_speed", lag("avg_speed", 1).over(cell_time_window))
    df = df.withColumn("prev_congestion_label", lag("congestion_label", 1).over(cell_time_window))
    df = df.withColumn("prev_2h_trip_count", lag("trip_count", 2).over(cell_time_window))
    df = df.withColumn("prev_2h_avg_speed", lag("avg_speed", 2).over(cell_time_window))
    
    return df


def create_temporal_features(df):
    """Create temporal features."""
    print("\nCreating Temporal Features...")
    
    cell_hour_window = Window.partitionBy("cell_lat", "cell_lon", "hour")
    
    df = df.withColumn("historical_avg_trips", avg("trip_count").over(cell_hour_window))
    df = df.withColumn("historical_avg_speed", avg("avg_speed").over(cell_hour_window))
    df = df.withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
    df = df.withColumn("is_rush_hour", when((col("hour").between(7, 9)) | (col("hour").between(17, 19)), 1).otherwise(0))
    df = df.withColumn("is_night", when((col("hour") >= 22) | (col("hour") <= 6), 1).otherwise(0))
    df = df.withColumn("is_manhattan_int", col("is_manhattan").cast(IntegerType()))
    
    return df


def prepare_final_features(df):
    """Prepare final feature set."""
    print("\nPreparing Final Features...")
    
    df = df.na.drop(subset=["prev_trip_count", "prev_avg_speed", "prev_congestion_label"])
    
    feature_columns = [
        "hour", "day_of_week", "month", "is_weekend", "is_rush_hour", "is_night",
        "cell_lat", "cell_lon", "is_manhattan_int",
        "prev_trip_count", "prev_avg_speed", "prev_congestion_label",
        "prev_2h_trip_count", "prev_2h_avg_speed",
        "historical_avg_trips", "historical_avg_speed"
    ]
    
    metadata_columns = ["cell_id", "hour_bucket", "year"]
    target_columns = ["congestion_label", "congestion_level", "avg_speed"]
    
    final_df = df.select(feature_columns + target_columns + metadata_columns)
    final_df = final_df.na.fill(0)
    
    # Add train/test split
    final_df = final_df.withColumn(
        "dataset_split",
        when(col("month").isin([1, 2]), "train")
        .when(col("month") == 3, "test")
        .otherwise("other")
    )
    
    print(f"  Final dataset: {final_df.count():,} records")
    return final_df, feature_columns


def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("SMART CITY TRAFFIC - DOCKER FEATURE ENGINEERING")
    print("=" * 60)
    
    start_time = datetime.now()
    spark = create_spark_session()
    
    df = load_cleaned_data(spark)
    agg_df = aggregate_by_cell_hour(df)
    agg_df = create_congestion_labels(agg_df)
    agg_df = create_lagged_features(agg_df)
    agg_df = create_temporal_features(agg_df)
    final_df, feature_columns = prepare_final_features(agg_df)
    
    output_path = f"{HDFS_NAMENODE}{HDFS_FEATURES_DIR}/training_features_spark.parquet"
    print(f"\nSaving to: {output_path}")
    final_df.write.mode("overwrite").parquet(output_path)
    print("  ✓ Saved successfully!")
    
    # Save feature columns locally for reference
    print("\nFeature columns:", feature_columns)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("FEATURE ENGINEERING COMPLETE")
    print("=" * 60)
    print(f"  Total records: {final_df.count():,}")
    print(f"  Features: {len(feature_columns)}")
    print(f"  Duration: {duration:.1f} seconds")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
