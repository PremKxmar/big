"""
============================================================
SMART CITY TRAFFIC - END-TO-END STREAMING PIPELINE TEST
============================================================
Validates the COMPLETE Kafka streaming pipeline:

    Producer ──► Kafka [taxi-trips] ──► Spark Structured Streaming
                                            │
                                            ▼
                              Kafka [traffic-predictions]
                                            │
                                            ▼
                                    API Bridge (consumer)

Phases:
    1. Pre-flight checks  – Docker containers, Kafka broker, topics
    2. Producer test      – Send N events to 'taxi-trips' topic
    3. Consumer test      – Spark reads from Kafka, creates windowed
                            aggregations, writes predictions back
    4. Bridge test        – API bridge consumes 'traffic-predictions'
                            and populates a shared traffic_state dict
    5. Validation         – Confirm end-to-end data flow and latency

Usage:
    cd backend
    python src/streaming/streaming_e2e_test.py
    python src/streaming/streaming_e2e_test.py --events 200 --timeout 120
    python src/streaming/streaming_e2e_test.py --skip-spark   # test Kafka only

Requirements:
    docker-compose up -d   (Kafka + Zookeeper must be running)
    pip install kafka-python pyspark
============================================================
"""

import json
import os
import sys
import time
import signal
import argparse
import subprocess
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_EVENTS = "taxi-trips"
TOPIC_PREDICTIONS = "traffic-predictions"
CONSUMER_GROUP_TEST = "e2e-test-consumer"

# Test parameters (overridable via CLI)
DEFAULT_NUM_EVENTS = 100
DEFAULT_TIMEOUT = 90       # seconds
PRODUCER_RATE = 50         # events/second
SPARK_TRIGGER_SEC = 5      # Spark micro-batch interval

# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 0 – UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

class TestResult:
    """Stores a single phase result."""
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.details: List[str] = []
        self.metrics: Dict = {}
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.time()
        print(f"\n{'═'*60}")
        print(f"  PHASE: {self.name}")
        print(f"{'═'*60}")

    def finish(self, passed: bool):
        self.end_time = time.time()
        self.passed = passed
        elapsed = self.end_time - self.start_time
        self.metrics["elapsed_seconds"] = round(elapsed, 2)
        icon = "✅" if passed else "❌"
        print(f"\n{icon}  {self.name} — {'PASSED' if passed else 'FAILED'} ({elapsed:.1f}s)")
        for d in self.details:
            print(f"   {d}")

    def log(self, msg: str):
        self.details.append(msg)
        print(f"   {msg}")


def _wait_for_condition(predicate, timeout, poll=1.0, desc="condition"):
    """Block until predicate() is truthy or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(poll)
    print(f"   ⏱  Timed out waiting for {desc} ({timeout}s)")
    return False


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 1 – PRE-FLIGHT CHECKS
# ═══════════════════════════════════════════════════════════════════════════

def phase_preflight() -> TestResult:
    """Check that Docker containers and Kafka broker are reachable."""
    result = TestResult("Pre-Flight Checks")
    result.start()

    # 1a. Check Docker containers
    try:
        out = subprocess.check_output(
            ["docker", "ps", "--format", "{{.Names}}"],
            stderr=subprocess.STDOUT, text=True, timeout=10
        )
        running = [c.strip() for c in out.strip().split("\n") if c.strip()]
        result.log(f"Docker containers running: {len(running)}")

        needed = {"zookeeper": False, "kafka": False}
        for name in running:
            for key in needed:
                if key in name.lower():
                    needed[key] = True
        for svc, found in needed.items():
            icon = "✓" if found else "✗"
            result.log(f"  {icon} {svc}: {'running' if found else 'NOT FOUND'}")

        if not all(needed.values()):
            result.log("⚠  Run: cd backend && docker-compose up -d")
            result.finish(False)
            return result
    except FileNotFoundError:
        result.log("Docker CLI not found – skipping container check")
    except subprocess.TimeoutExpired:
        result.log("Docker command timed out")
    except Exception as e:
        result.log(f"Docker check error: {e}")

    # 1b. Check Kafka broker reachability
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        p.close()
        result.log(f"✓ Kafka broker reachable at {KAFKA_BOOTSTRAP}")
    except (NoBrokersAvailable, Exception) as e:
        result.log(f"✗ Cannot reach Kafka broker: {e}")
        result.finish(False)
        return result

    # 1c. Ensure topics exist (Kafka auto-creates by default, but be explicit)
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        existing = admin.list_topics()
        for topic in [TOPIC_EVENTS, TOPIC_PREDICTIONS]:
            if topic in existing:
                result.log(f"✓ Topic '{topic}' exists")
            else:
                admin.create_topics([
                    NewTopic(name=topic, num_partitions=3, replication_factor=1)
                ])
                result.log(f"✓ Topic '{topic}' created")
        admin.close()
    except Exception as e:
        result.log(f"Topic check warning (non-fatal): {e}")

    result.finish(True)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 2 – PRODUCER TEST
# ═══════════════════════════════════════════════════════════════════════════

def _generate_test_event(seq: int) -> Dict:
    """Generate a deterministic test trip event."""
    import random
    random.seed(seq)
    now = datetime.now()
    pickup_lat = 40.7 + random.uniform(-0.1, 0.1)
    pickup_lon = -73.97 + random.uniform(-0.1, 0.1)
    dropoff_lat = pickup_lat + random.uniform(-0.03, 0.03)
    dropoff_lon = pickup_lon + random.uniform(-0.03, 0.03)
    distance = random.uniform(0.5, 10)
    duration = random.uniform(5, 45)
    speed = (distance / (duration / 60)) if duration > 0 else 10
    speed = max(2, min(55, speed))
    hour = now.hour
    dow = now.weekday() + 1
    cell_lat = int((pickup_lat - 40.4774) / 0.01)
    cell_lon = int((pickup_lon - (-74.2591)) / 0.01)
    cell_id = f"{cell_lat}_{cell_lon}"

    return {
        "event_id": f"e2e_test_{seq}_{int(time.time()*1000)}",
        "event_time": now.isoformat(),
        "pickup_datetime": (now - timedelta(minutes=duration)).isoformat(),
        "dropoff_datetime": now.isoformat(),
        "pickup_lat": round(pickup_lat, 6),
        "pickup_lon": round(pickup_lon, 6),
        "dropoff_lat": round(dropoff_lat, 6),
        "dropoff_lon": round(dropoff_lon, 6),
        "trip_distance": round(distance, 2),
        "duration_minutes": round(duration, 2),
        "speed_mph": round(speed, 2),
        "passenger_count": random.randint(1, 4),
        "fare_amount": round(random.uniform(5, 80), 2),
        "cell_id": cell_id,
        "cell_lat": cell_lat,
        "cell_lon": cell_lon,
        "hour": hour,
        "day_of_week": dow,
        "month": now.month,
        "year": now.year,
        "is_weekend": 1 if dow >= 6 else 0,
        "is_rush_hour": 1 if hour in [7, 8, 9, 17, 18, 19] else 0,
        "is_night": 1 if hour >= 22 or hour <= 5 else 0,
        "is_manhattan": 1 if (40.7 <= pickup_lat <= 40.82 and
                              -74.02 <= pickup_lon <= -73.93) else 0,
    }


def phase_producer(num_events: int) -> TestResult:
    """Send N test events to the taxi-trips Kafka topic."""
    from kafka import KafkaProducer
    result = TestResult(f"Producer → Kafka ({num_events} events)")
    result.start()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )

    sent = 0
    errors = 0
    t0 = time.time()
    interval = 1.0 / PRODUCER_RATE

    for i in range(num_events):
        event = _generate_test_event(i)
        try:
            producer.send(TOPIC_EVENTS, key=event["cell_id"], value=event)
            sent += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                result.log(f"  Send error: {e}")
        if interval > 0:
            time.sleep(interval)

    producer.flush()
    producer.close()
    elapsed = time.time() - t0
    rate = sent / elapsed if elapsed > 0 else 0

    result.metrics.update({"events_sent": sent, "errors": errors,
                           "rate_per_sec": round(rate, 1)})
    result.log(f"Sent {sent:,} events in {elapsed:.1f}s ({rate:.0f} evt/s)")
    result.log(f"Errors: {errors}")
    result.finish(sent == num_events and errors == 0)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 3 – SPARK STRUCTURED STREAMING CONSUMER
# ═══════════════════════════════════════════════════════════════════════════

def phase_spark_consumer(timeout: int) -> TestResult:
    """
    Start Spark Structured Streaming:
        - Read from 'taxi-trips'
        - Apply windowed aggregation
        - Write predictions to 'traffic-predictions' Kafka topic
    Run for `timeout` seconds then stop gracefully.
    """
    result = TestResult("Spark Structured Streaming Consumer")
    result.start()

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import (
            col, from_json, window, count, avg,
            max as spark_max, min as spark_min,
            when, struct, to_json, to_timestamp
        )
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType,
            IntegerType
        )
        from pyspark.ml import PipelineModel
    except ImportError as e:
        result.log(f"PySpark not installed: {e}")
        result.finish(False)
        return result

    # 3a. Create Spark session
    result.log("Creating Spark session for streaming test...")

    # Windows Hadoop workaround
    if os.name == "nt":
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

    spark = (
        SparkSession.builder
        .appName("E2E-Streaming-Test")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation",
                str(PROJECT_ROOT / "data" / "checkpoints" / "e2e_test"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    result.log(f"Spark {spark.version} on {spark.sparkContext.master}")

    # 3b. Optionally load ML model
    model = None
    model_path = str(PROJECT_ROOT / "models" / "spark_congestion_model")
    try:
        model = PipelineModel.load(model_path)
        result.log(f"✓ ML model loaded from {model_path}")
    except Exception:
        result.log("⚠ ML model not found – using rule-based predictions")

    # 3c. Define schema matching producer events
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("pickup_lat", DoubleType(), True),
        StructField("pickup_lon", DoubleType(), True),
        StructField("dropoff_lat", DoubleType(), True),
        StructField("dropoff_lon", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("duration_minutes", DoubleType(), True),
        StructField("speed_mph", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("cell_id", StringType(), True),
        StructField("cell_lat", IntegerType(), True),
        StructField("cell_lon", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("is_weekend", IntegerType(), True),
        StructField("is_rush_hour", IntegerType(), True),
        StructField("is_night", IntegerType(), True),
        StructField("is_manhattan", IntegerType(), True),
    ])

    # 3d. Read stream from Kafka
    result.log(f"Subscribing to Kafka topic: {TOPIC_EVENTS}")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_EVENTS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("timestamp").alias("kafka_ts"),
        )
        .select(
            "kafka_key", "kafka_ts",
            from_json(col("kafka_value"), schema).alias("d"),
        )
        .select("kafka_key", "kafka_ts", "d.*")
    )

    # 3e. Add rule-based / ML prediction
    if model is not None:
        predicted = model.transform(parsed).withColumn(
            "congestion_level",
            when(col("prediction") == 0, "Low")
            .when(col("prediction") == 1, "Medium")
            .otherwise("High"),
        )
    else:
        predicted = parsed.withColumn(
            "congestion_level",
            when((col("is_rush_hour") == 1) & (col("is_manhattan") == 1), "High")
            .when((col("is_rush_hour") == 1) | (col("speed_mph") < 10), "Medium")
            .otherwise("Low"),
        ).withColumn(
            "prediction",
            when(col("congestion_level") == "Low", 0)
            .when(col("congestion_level") == "Medium", 1)
            .otherwise(2),
        )

    # 3f. Windowed aggregation (1-minute windows)
    agg = (
        predicted
        .withColumn("event_ts", to_timestamp(col("event_time")))
        .withWatermark("event_ts", "2 minutes")
        .groupBy(
            window(col("event_ts"), "1 minute"),
            col("cell_id"), col("cell_lat"), col("cell_lon"),
        )
        .agg(
            count("*").alias("trip_count"),
            avg("speed_mph").alias("avg_speed"),
            spark_max("speed_mph").alias("max_speed"),
            spark_min("speed_mph").alias("min_speed"),
            avg("trip_distance").alias("avg_distance"),
            avg("fare_amount").alias("avg_fare"),
        )
        .withColumn(
            "congestion_level",
            when(col("avg_speed") < 8, "High")
            .when(col("avg_speed") < 15, "Medium")
            .otherwise("Low"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    # 3g. Write aggregated predictions to Kafka (traffic-predictions topic)
    checkpoint_path = str(PROJECT_ROOT / "data" / "checkpoints" / "e2e_kafka_out")
    # Clean up stale checkpoint for idempotent reruns
    import shutil
    if os.path.isdir(checkpoint_path):
        shutil.rmtree(checkpoint_path, ignore_errors=True)

    kafka_out = agg.select(
        col("cell_id").alias("key"),
        to_json(struct("*")).alias("value"),
    )

    result.log(f"Writing aggregated predictions → {TOPIC_PREDICTIONS}")
    query = (
        kafka_out.writeStream
        .outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_PREDICTIONS)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{SPARK_TRIGGER_SEC} seconds")
        .start()
    )

    # Also write to console for observability
    console_checkpoint = str(PROJECT_ROOT / "data" / "checkpoints" / "e2e_console")
    if os.path.isdir(console_checkpoint):
        shutil.rmtree(console_checkpoint, ignore_errors=True)

    console_query = (
        agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime=f"{SPARK_TRIGGER_SEC} seconds")
        .start()
    )

    # 3h. Let the streaming job run for the specified timeout
    result.log(f"Streaming running for up to {timeout}s …")
    try:
        query.awaitTermination(timeout)
    except Exception as e:
        result.log(f"Streaming error: {e}")

    # Collect status before stopping
    kafka_status = query.status
    is_active = query.isActive
    recent_progress = query.recentProgress  # list of recent micro-batch stats

    # Stop queries
    for q in [query, console_query]:
        try:
            q.stop()
        except Exception:
            pass

    spark.stop()

    # Evaluate
    batches_processed = len(recent_progress) if recent_progress else 0
    total_input_rows = sum(
        p.get("numInputRows", 0) for p in (recent_progress or [])
    )

    result.metrics.update({
        "batches_processed": batches_processed,
        "total_input_rows": total_input_rows,
        "spark_was_active": is_active,
        "last_status": str(kafka_status),
    })
    result.log(f"Micro-batches processed: {batches_processed}")
    result.log(f"Total input rows consumed: {total_input_rows}")

    passed = batches_processed > 0 or total_input_rows > 0
    result.finish(passed)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 4 – KAFKA API BRIDGE
# ═══════════════════════════════════════════════════════════════════════════

def phase_bridge(timeout: int) -> TestResult:
    """
    Consume from 'traffic-predictions' topic and verify that
    records are populated into a shared traffic_state dictionary.
    """
    result = TestResult("Kafka → API Bridge Consumer")
    result.start()

    try:
        from kafka import KafkaConsumer
    except ImportError:
        result.log("kafka-python not installed")
        result.finish(False)
        return result

    traffic_state: Dict = {}
    messages: List[Dict] = []

    result.log(f"Consuming from topic: {TOPIC_PREDICTIONS}")
    result.log(f"Consumer group: {CONSUMER_GROUP_TEST}")

    try:
        consumer = KafkaConsumer(
            TOPIC_PREDICTIONS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP_TEST,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=2000,
            max_poll_records=500,
        )
    except Exception as e:
        result.log(f"Cannot connect: {e}")
        result.finish(False)
        return result

    deadline = time.time() + timeout
    while time.time() < deadline:
        for msg in consumer:
            data = msg.value
            cell_id = data.get("cell_id", msg.key.decode("utf-8") if msg.key else "unknown")
            traffic_state[cell_id] = {
                "cell_id": cell_id,
                "trip_count": data.get("trip_count", 0),
                "avg_speed": data.get("avg_speed", 0),
                "congestion_level": data.get("congestion_level", "Unknown"),
                "source": "kafka_e2e_test",
            }
            messages.append(data)
        if messages:
            break  # got at least some messages
        time.sleep(1)

    consumer.close()

    result.metrics.update({
        "messages_consumed": len(messages),
        "cells_in_state": len(traffic_state),
    })
    result.log(f"Messages consumed: {len(messages)}")
    result.log(f"Distinct cells populated: {len(traffic_state)}")

    if traffic_state:
        sample = list(traffic_state.values())[:3]
        for s in sample:
            result.log(f"  Cell {s['cell_id']}: "
                        f"trips={s['trip_count']}, "
                        f"speed={s['avg_speed']:.1f}, "
                        f"level={s['congestion_level']}")

    result.finish(len(messages) > 0)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 5 – STANDALONE KAFKA ROUND-TRIP (no Spark)
# ═══════════════════════════════════════════════════════════════════════════

def phase_kafka_roundtrip(num_events: int = 20) -> TestResult:
    """
    Quick Kafka-only round-trip: produce → consume on the same topic.
    Validates Kafka connectivity independent of Spark.
    """
    from kafka import KafkaProducer, KafkaConsumer

    result = TestResult("Kafka Round-Trip (no Spark)")
    result.start()

    test_topic = "e2e-test-roundtrip"

    # Produce
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    for i in range(num_events):
        producer.send(test_topic, value={"seq": i, "ts": time.time()})
    producer.flush()
    producer.close()
    result.log(f"Produced {num_events} messages to '{test_topic}'")

    # Consume
    consumer = KafkaConsumer(
        test_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"e2e-roundtrip-{int(time.time())}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    received = []
    for msg in consumer:
        received.append(msg.value)
    consumer.close()

    result.metrics.update({"produced": num_events, "received": len(received)})
    result.log(f"Consumed {len(received)} messages from '{test_topic}'")
    result.finish(len(received) >= num_events)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN – ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

def print_report(results: List[TestResult]):
    """Print a summary table of all phases."""
    print("\n")
    print("╔" + "═"*62 + "╗")
    print("║" + " END-TO-END STREAMING PIPELINE TEST REPORT".center(62) + "║")
    print("╠" + "═"*62 + "╣")
    all_pass = True
    for r in results:
        icon = "✅" if r.passed else "❌"
        elapsed = r.metrics.get("elapsed_seconds", 0)
        line = f"  {icon}  {r.name:<40} {elapsed:>6.1f}s"
        print("║" + line.ljust(62) + "║")
        if not r.passed:
            all_pass = False
    print("╠" + "═"*62 + "╣")
    overall = "ALL PHASES PASSED ✅" if all_pass else "SOME PHASES FAILED ❌"
    print("║" + f"  OVERALL: {overall}".ljust(62) + "║")
    print("╚" + "═"*62 + "╝")

    # Detailed metrics
    print("\n📊 Detailed Metrics:")
    for r in results:
        if r.metrics:
            print(f"\n  [{r.name}]")
            for k, v in r.metrics.items():
                print(f"    {k}: {v}")

    print(f"\n⏱  Test completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return all_pass


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end streaming pipeline test for Smart City Traffic",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--events", type=int, default=DEFAULT_NUM_EVENTS,
                        help=f"Number of test events to produce (default {DEFAULT_NUM_EVENTS})")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"Spark streaming timeout in seconds (default {DEFAULT_TIMEOUT})")
    parser.add_argument("--skip-spark", action="store_true",
                        help="Skip the Spark streaming phase (Kafka-only test)")
    parser.add_argument("--skip-bridge", action="store_true",
                        help="Skip the API bridge consumer phase")
    args = parser.parse_args()

    print("""
╔══════════════════════════════════════════════════════════════╗
║     SMART CITY TRAFFIC – END-TO-END STREAMING PIPELINE TEST ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  Producer ──► Kafka [taxi-trips]                             ║
║                  │                                           ║
║                  ▼                                           ║
║  Spark Structured Streaming (window agg + predictions)       ║
║                  │                                           ║
║                  ▼                                           ║
║  Kafka [traffic-predictions] ──► API Bridge ──► traffic_state║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""")
    print(f"  Events:   {args.events}")
    print(f"  Timeout:  {args.timeout}s")
    print(f"  Spark:    {'SKIP' if args.skip_spark else 'ENABLED'}")
    print(f"  Bridge:   {'SKIP' if args.skip_bridge else 'ENABLED'}")
    print(f"  Time:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    results: List[TestResult] = []

    # ── Phase 1: Pre-flight ──
    r1 = phase_preflight()
    results.append(r1)
    if not r1.passed:
        print("\n⛔ Pre-flight checks failed. Fix issues above and retry.")
        print_report(results)
        sys.exit(1)

    # ── Phase 2: Kafka round-trip (quick) ──
    r_rt = phase_kafka_roundtrip(num_events=20)
    results.append(r_rt)

    # ── Phase 3: Producer ──
    r2 = phase_producer(num_events=args.events)
    results.append(r2)

    if not args.skip_spark:
        # ── Phase 4: Spark Structured Streaming ──
        # We need to also produce *while* Spark is listening (since startingOffsets=latest).
        # Strategy: start producer in a background thread, then run Spark consumer.
        bg_events = max(50, args.events // 2)
        bg_done = threading.Event()

        def _bg_produce():
            """Background producer that runs concurrently with Spark consumer."""
            time.sleep(5)  # Let Spark start first
            try:
                from kafka import KafkaProducer as KP
                p = KP(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                )
                interval = 1.0 / PRODUCER_RATE
                for i in range(bg_events):
                    evt = _generate_test_event(1000 + i)
                    p.send(TOPIC_EVENTS, key=evt["cell_id"], value=evt)
                    time.sleep(interval)
                p.flush()
                p.close()
                print(f"\n   🔄 Background producer finished ({bg_events} events)")
            except Exception as e:
                print(f"\n   ⚠ Background producer error: {e}")
            finally:
                bg_done.set()

        bg_thread = threading.Thread(target=_bg_produce, daemon=True)
        bg_thread.start()

        r3 = phase_spark_consumer(timeout=args.timeout)
        results.append(r3)

        bg_done.wait(timeout=10)

    if not args.skip_bridge:
        # ── Phase 5: API Bridge consumer ──
        r4 = phase_bridge(timeout=20)
        results.append(r4)

    # ── Report ──
    all_pass = print_report(results)
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
