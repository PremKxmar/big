#!/bin/bash
# =============================================================================
# Smart City Traffic - Spark Pipeline Runner (Docker)
# =============================================================================
# This script runs the entire data processing pipeline inside Docker.
# It uses spark-submit to run jobs on the Spark cluster.
#
# Usage:
#   docker exec spark-master bash /app/run_pipeline.sh
# =============================================================================

set -e  # Exit on error

echo "============================================================"
echo "SMART CITY TRAFFIC - SPARK CLUSTER PIPELINE"
echo "============================================================"
echo "Running inside Docker container: $(hostname)"
echo "Spark Master: spark://spark-master:7077"
echo "HDFS Namenode: hdfs://namenode:9000"
echo ""

# Common spark-submit settings
SPARK_SUBMIT="/opt/spark/bin/spark-submit"
MASTER="spark://spark-master:7077"
HDFS_CONF="--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000"
DEPLOY_MODE="--deploy-mode client"
APP_DIR="/app/src/batch"

# Step 1: Data Cleaning
echo ""
echo "============================================================"
echo "STEP 1: DATA CLEANING"
echo "============================================================"
$SPARK_SUBMIT $DEPLOY_MODE --master $MASTER $HDFS_CONF \
    $APP_DIR/docker_data_cleaning.py

# Step 2: Feature Engineering
echo ""
echo "============================================================"
echo "STEP 2: FEATURE ENGINEERING"
echo "============================================================"
$SPARK_SUBMIT $DEPLOY_MODE --master $MASTER $HDFS_CONF \
    $APP_DIR/docker_feature_engineering.py

# Step 3: Model Training
echo ""
echo "============================================================"
echo "STEP 3: MODEL TRAINING"
echo "============================================================"
$SPARK_SUBMIT $DEPLOY_MODE --master $MASTER $HDFS_CONF \
    $APP_DIR/docker_model_training.py

echo ""
echo "============================================================"
echo "PIPELINE COMPLETE!"
echo "============================================================"
echo "All steps finished successfully."
echo "Check HDFS for processed data and trained model."
echo ""
