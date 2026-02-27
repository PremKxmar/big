#!/usr/bin/env python3
"""
Smart City Traffic - Docker Model Training Script
===================================================
Standalone version for Docker container execution.
"""

import os
import sys
from datetime import datetime
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# =============================================================================
# CONFIGURATION (Hardcoded for Docker)
# =============================================================================
HDFS_NAMENODE = "hdfs://namenode:9000"
HDFS_FEATURES_DIR = "/smart-city-traffic/data/features"
HDFS_MODELS_DIR = "/smart-city-traffic/data/models"

RANDOM_SEED = 42


def create_spark_session():
    """Create Spark session for cluster mode."""
    spark = SparkSession.builder \
        .appName("SmartCityTraffic-ModelTraining-Docker") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_training_data(spark):
    """Load training features."""
    features_path = f"{HDFS_NAMENODE}{HDFS_FEATURES_DIR}/training_features_spark.parquet"
    print(f"\nLoading training data from: {features_path}")
    
    df = spark.read.parquet(features_path)
    print(f"  ✓ Loaded {df.count():,} samples")
    return df


def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("SMART CITY TRAFFIC - DOCKER MODEL TRAINING")
    print("=" * 60)
    
    start_time = datetime.now()
    spark = create_spark_session()
    
    df = load_training_data(spark)
    
    # Feature columns
    feature_columns = [
        "hour", "day_of_week", "month", "is_weekend", "is_rush_hour", "is_night",
        "cell_lat", "cell_lon", "is_manhattan_int",
        "prev_trip_count", "prev_avg_speed", "prev_congestion_label",
        "prev_2h_trip_count", "prev_2h_avg_speed",
        "historical_avg_trips", "historical_avg_speed"
    ]
    
    # Train/Test split
    print("\nCreating Train/Test Split...")
    train_df = df.filter(col("dataset_split") == "train")
    test_df = df.filter(col("dataset_split") == "test")
    
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"  Training set: {train_count:,} samples")
    print(f"  Test set: {test_count:,} samples")
    
    if train_count < 100 or test_count < 100:
        print("  Using random split instead...")
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=RANDOM_SEED)
    
    # Create pipeline
    print("\nCreating ML Pipeline...")
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_raw", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    rf = RandomForestClassifier(
        featuresCol="features", labelCol="congestion_label",
        numTrees=100, maxDepth=10, seed=RANDOM_SEED
    )
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    
    # Train
    print("\nTraining Model...")
    model = pipeline.fit(train_df)
    print("  ✓ Training complete!")
    
    # Evaluate
    print("\nEvaluating Model...")
    test_predictions = model.transform(test_df)
    
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="congestion_label", predictionCol="prediction", metricName="accuracy"
    )
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="congestion_label", predictionCol="prediction", metricName="f1"
    )
    
    test_accuracy = accuracy_evaluator.evaluate(test_predictions)
    test_f1 = f1_evaluator.evaluate(test_predictions)
    
    print(f"\n  TEST ACCURACY: {test_accuracy:.4f}")
    print(f"  TEST F1-SCORE: {test_f1:.4f}")
    
    # Save model
    model_path = f"{HDFS_NAMENODE}{HDFS_MODELS_DIR}/spark_congestion_model"
    print(f"\nSaving model to: {model_path}")
    model.write().overwrite().save(model_path)
    print("  ✓ Model saved!")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)
    print(f"  Duration: {duration:.1f} seconds")
    print(f"  Test Accuracy: {test_accuracy:.4f}")
    print(f"  Model saved to: {model_path}")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
