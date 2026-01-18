#!/bin/bash
# ============================================
# Run Scala Preprocessor via Docker Spark
# ============================================
#
# This script runs the Scala TrafficDataPreprocessor
# using the Docker Spark master container.
#
# Usage:
#   ./run_scala_preprocessor.sh           # Local mode
#   ./run_scala_preprocessor.sh --hdfs    # HDFS mode
#
# Prerequisites:
#   - Docker containers running (spark-master, namenode)
#   - docker-compose up -d spark-master namenode datanode
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALA_FILE="$SCRIPT_DIR/scala/TrafficDataPreprocessor.scala"

# Check if Docker is running
if ! docker ps | grep -q spark-master; then
    echo "ERROR: spark-master container is not running!"
    echo "Run: docker-compose up -d spark-master"
    exit 1
fi

echo "============================================"
echo "RUNNING SCALA PREPROCESSOR VIA DOCKER"
echo "============================================"
echo "Arguments: $@"
echo ""

# Copy Scala file to container
echo "Copying Scala file to spark-master container..."
docker cp "$SCALA_FILE" spark-master:/tmp/TrafficDataPreprocessor.scala

# Run via spark-shell
echo "Running Scala preprocessor..."
docker exec -it spark-master /opt/spark/bin/spark-shell \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf "spark.sql.parquet.compression.codec=snappy" \
    -I /tmp/TrafficDataPreprocessor.scala \
    -e "TrafficDataPreprocessor.main(Array(\"$@\"))"

echo ""
echo "============================================"
echo "SCALA PREPROCESSING COMPLETE"
echo "============================================"
