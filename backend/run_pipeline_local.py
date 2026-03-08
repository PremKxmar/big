"""
============================================================
SMART CITY TRAFFIC – MASTER PIPELINE ORCHESTRATOR (Cluster)
============================================================

Single command to run the COMPLETE Big Data pipeline end-to-end
in CLUSTER MODE (Spark Master + Workers via Docker):

    Step 1  ─  Data Cleaning         (PySpark DataFrame API → Spark Cluster)
    Step 2  ─  Feature Engineering    (PySpark Window + Agg → Spark Cluster)
    Step 3  ─  Model Training         (RF vs GBT vs LR → Spark Cluster)
    Step 4  ─  Start Flask API        (serves Spark MLlib predictions via Cluster)
    Step 5  ─  (Optional) Kafka Streaming demo

Prerequisites:
    docker-compose up -d   (start HDFS, Spark, Kafka containers)

Usage:
    cd backend
    python run_pipeline_local.py                   # Steps 1-4 (cluster mode)
    python run_pipeline_local.py --skip-training   # Steps 1-2, then API
    python run_pipeline_local.py --api-only        # Just start API
    python run_pipeline_local.py --all             # Steps 1-5 (Kafka too)
    python run_pipeline_local.py --local           # Run in local mode (no Docker)

Requirements:
    pip install -r requirements.txt
    Java 8/11/17 + JAVA_HOME set
    docker-compose up -d   (HDFS + Spark + Kafka)
============================================================
"""

import os
import sys
import time
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# Paths
BACKEND_ROOT = Path(__file__).parent
SRC_DIR = BACKEND_ROOT / "src"
BATCH_DIR = SRC_DIR / "batch"
API_DIR = SRC_DIR / "api"
STREAMING_DIR = SRC_DIR / "streaming"
DATA_DIR = BACKEND_ROOT / "data"
MODELS_DIR = BACKEND_ROOT / "models"

PYTHON = sys.executable  # Use the same Python that launched this script


def banner(title: str, char="═"):
    width = 60
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")


def run_step(label: str, script_path: Path, extra_args: list = None):
    """Run a Python script as a subprocess, streaming output in real time."""
    banner(f"STEP: {label}")
    cmd = [PYTHON, str(script_path)] + (extra_args or [])
    print(f"  CMD: {' '.join(cmd)}")
    print(f"  CWD: {BACKEND_ROOT}\n")

    start = time.time()
    result = subprocess.run(
        cmd,
        cwd=str(BACKEND_ROOT),
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n  ✗ FAILED (exit code {result.returncode}) after {elapsed:.1f}s")
        return False, elapsed
    print(f"\n  ✓ Completed in {elapsed:.1f}s")
    return True, elapsed


def check_prerequisites():
    """Quick sanity checks before running the pipeline."""
    banner("PRE-FLIGHT CHECKS", "─")

    errors = []

    # Check raw data
    raw_dir = Path(r"c:\sem6-real\bigdata\vscode")
    csv_files = list(raw_dir.glob("yellow_tripdata_*.csv")) if raw_dir.exists() else []
    if csv_files:
        total_gb = sum(f.stat().st_size for f in csv_files) / (1024 ** 3)
        print(f"  ✓ Raw CSV files: {len(csv_files)} ({total_gb:.1f} GB)")
    else:
        # Check alternative location (data/raw)
        alt_raw = BACKEND_ROOT / "data" / "raw"
        alt_csvs = list(alt_raw.glob("yellow_tripdata_*.csv")) if alt_raw.exists() else []
        if alt_csvs:
            print(f"  ✓ Raw CSV files (data/raw): {len(alt_csvs)}")
        else:
            errors.append("No raw CSV files found. Place yellow_tripdata_*.csv in data/raw/")

    # Check processed data (optional — will be created by step 1)
    parquet_files = list((DATA_DIR / "processed").glob("*_clean.parquet")) if (DATA_DIR / "processed").exists() else []
    if parquet_files:
        print(f"  ✓ Processed Parquet files: {len(parquet_files)} (can skip cleaning)")
    else:
        print(f"  ⚠ No processed data yet — Step 1 will create it")

    # Check model (optional — will be created by step 3)
    model_dir = MODELS_DIR / "spark_congestion_model"
    if model_dir.exists():
        print(f"  ✓ Trained model exists at: {model_dir}")
    else:
        print(f"  ⚠ No trained model yet — Step 3 will create it")

    # Check Java
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home:
        print(f"  ✓ JAVA_HOME: {java_home}")
    else:
        print(f"  ⚠ JAVA_HOME not set (PySpark may fail)")

    # Check Hadoop (Windows)
    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
        winutils = Path(hadoop_home) / "bin" / "winutils.exe"
        if winutils.exists():
            print(f"  ✓ winutils.exe found at: {winutils}")
        else:
            print(f"  ⚠ winutils.exe not found at {winutils} (PySpark local may warn)")

    if errors:
        for e in errors:
            print(f"  ✗ {e}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Smart City Traffic — Master Pipeline Orchestrator (Cluster Mode)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--skip-cleaning", action="store_true",
                        help="Skip Step 1 (data cleaning) if Parquet files exist")
    parser.add_argument("--skip-features", action="store_true",
                        help="Skip Step 2 (feature engineering)")
    parser.add_argument("--skip-training", action="store_true",
                        help="Skip Step 3 (model training)")
    parser.add_argument("--api-only", action="store_true",
                        help="Only start the API server (skip Steps 1-3)")
    parser.add_argument("--all", action="store_true",
                        help="Run everything including Kafka streaming demo")
    parser.add_argument("--local", action="store_true",
                        help="Run in local mode (no Docker required)")
    args = parser.parse_args()
    
    # Determine execution mode
    mode = "LOCAL" if args.local else "CLUSTER"
    extra_args = ["--local", "--no-hdfs"] if args.local else []

    print(r"""
   _____ __  __    _    ____ _____    ____ ___ _______   __
  / ____|  \/  |  / \  |  _ \_   _|  / ___|_ _|_   _\ \ / /
  \___ \| |\/| | / _ \ | |_) || |   | |    | |  | |  \ V / 
   ___) | |  | |/ ___ \|  _ < | |   | |___ | |  | |   | |  
  |____/|_|  |_/_/   \_\_| \_\|_|    \____|___| |_|   |_|  
                                                            
  ╔══════════════════════════════════════════════════════╗
  ║  BIG DATA TRAFFIC ANALYTICS – CLUSTER PIPELINE      ║
  ╚══════════════════════════════════════════════════════╝
""")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Mode: {mode} ({'spark://localhost:7077 + HDFS' if mode == 'CLUSTER' else 'local[*]'})")
    print(f"  Python: {sys.executable}")
    print(f"  Backend: {BACKEND_ROOT}")

    if not check_prerequisites() and not args.api_only:
        print("\n  ⛔ Fix the issues above before running the pipeline.")
        sys.exit(1)

    results = []
    pipeline_start = time.time()

    # ── Step 1: Data Cleaning ──
    if not args.api_only and not args.skip_cleaning:
        has_parquet = list((DATA_DIR / "processed").glob("*_clean.parquet")) if (DATA_DIR / "processed").exists() else []
        if has_parquet and not args.all:
            print(f"\n  ⏭  Skipping data cleaning ({len(has_parquet)} Parquet files exist)")
            print(f"     Use --all to force re-run")
            results.append(("Data Cleaning", True, 0))
        else:
            ok, t = run_step(
                f"Data Cleaning (PySpark DataFrame — {mode})",
                BATCH_DIR / "data_cleaning_spark.py",
                extra_args
            )
            results.append(("Data Cleaning", ok, t))
            if not ok:
                print("\n  ⛔ Data cleaning failed. Cannot continue.")
                sys.exit(1)

    # ── Step 2: Feature Engineering ──
    if not args.api_only and not args.skip_features:
        ok, t = run_step(
            f"Feature Engineering (PySpark Window + Aggregation — {mode})",
            BATCH_DIR / "feature_engineering_spark.py",
            extra_args
        )
        results.append(("Feature Engineering", ok, t))
        if not ok:
            print("\n  ⛔ Feature engineering failed. Cannot continue.")
            sys.exit(1)

    # ── Step 3: Model Training (RF vs GBT vs LR comparison) ──
    if not args.api_only and not args.skip_training:
        ok, t = run_step(
            f"Model Training (RF vs GBT vs LR + Confusion Matrix — {mode})",
            BATCH_DIR / "model_training_spark.py",
            extra_args
        )
        results.append(("Model Training", ok, t))
        if not ok:
            print("\n  ⚠ Model training failed. API will use rule-based fallback.")

    # ── Step 4: RDD Analysis Demo ──
    if not args.api_only and args.all:
        ok, t = run_step(
            f"RDD Analysis on Real Traffic Data — {mode}",
            BATCH_DIR / "traffic_rdd_analysis.py",
            extra_args
        )
        results.append(("RDD Analysis", ok, t))

    # ── Pipeline Summary ──
    pipeline_time = time.time() - pipeline_start

    if results:
        banner("PIPELINE SUMMARY")
        print(f"\n  {'Step':<30} {'Status':>8}  {'Time':>8}")
        print(f"  {'─'*30} {'─'*8}  {'─'*8}")
        for name, ok, t in results:
            status = "✓ PASS" if ok else "✗ FAIL"
            print(f"  {name:<30} {status:>8}  {t:>7.1f}s")
        print(f"  {'─'*30} {'─'*8}  {'─'*8}")
        print(f"  {'Total':<30} {'':>8}  {pipeline_time:>7.1f}s")

    # ── Step 5: Start API Server ──
    banner("STARTING API SERVER")
    print(f"  Flask API with Spark MLlib model predictions")
    print(f"  URL: http://localhost:5000")
    print(f"  Dashboard: http://localhost:5000/api/health\n")

    try:
        # Run the API in the foreground (blocks until Ctrl+C)
        subprocess.run(
            [PYTHON, str(API_DIR / "app.py")],
            cwd=str(BACKEND_ROOT),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
    except KeyboardInterrupt:
        print("\n\n  🛑 API Server stopped.")

    print("\n  Pipeline finished. Goodbye!")


if __name__ == "__main__":
    main()
