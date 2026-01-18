# ============================================
# Run Scala Preprocessor via Docker Spark
# ============================================
#
# This script runs the Scala TrafficDataPreprocessor
# using the Docker Spark master container.
#
# Usage:
#   .\run_scala_preprocessor.ps1           # Local mode
#   .\run_scala_preprocessor.ps1 --hdfs    # HDFS mode
#
# Prerequisites:
#   - Docker containers running (spark-master, namenode)
#   - docker-compose up -d spark-master namenode datanode
# ============================================

param(
    [switch]$hdfs,
    [switch]$cluster
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ScalaFile = Join-Path $ScriptDir "scala\TrafficDataPreprocessor.scala"

# Check if Docker is running
$sparkMasterRunning = docker ps --filter "name=spark-master" --format "{{.Names}}" 2>$null
if (-not $sparkMasterRunning) {
    Write-Host "ERROR: spark-master container is not running!" -ForegroundColor Red
    Write-Host "Run: docker-compose up -d spark-master" -ForegroundColor Yellow
    exit 1
}

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "RUNNING SCALA PREPROCESSOR VIA DOCKER" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Build arguments
$args_list = @()
if ($hdfs) { 
    $args_list += "--hdfs" 
    Write-Host "Mode: HDFS" -ForegroundColor Green
} else {
    Write-Host "Mode: Local" -ForegroundColor Green
}
if ($cluster) { 
    $args_list += "--cluster" 
}

$argsString = $args_list -join " "
Write-Host "Arguments: $argsString"
Write-Host ""

# Copy Scala file to container
Write-Host "Copying Scala file to spark-master container..." -ForegroundColor Yellow
docker cp $ScalaFile spark-master:/tmp/TrafficDataPreprocessor.scala

# Create a temp script to run the Scala code
$tempScript = @"
:load /tmp/TrafficDataPreprocessor.scala
TrafficDataPreprocessor.main(Array($($args_list | ForEach-Object { "`"$_`"" } | Join-String -Separator ", ")))
:quit
"@

# Write temp script
$tempScriptPath = Join-Path $env:TEMP "run_scala.txt"
$tempScript | Out-File -FilePath $tempScriptPath -Encoding ASCII

# Copy script to container
docker cp $tempScriptPath spark-master:/tmp/run_scala.txt

# Run via spark-shell
Write-Host "Running Scala preprocessor..." -ForegroundColor Yellow
Write-Host ""

docker exec spark-master /opt/spark/bin/spark-shell `
    --master local[*] `
    --driver-memory 4g `
    --conf "spark.sql.parquet.compression.codec=snappy" `
    -i /tmp/run_scala.txt

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "SCALA PREPROCESSING COMPLETE" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
