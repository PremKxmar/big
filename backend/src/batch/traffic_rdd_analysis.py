"""
============================================================
SMART CITY TRAFFIC - RDD API ANALYSIS ON REAL TRAFFIC DATA
============================================================

This script demonstrates the RDD API on ACTUAL NYC taxi trip data
(not toy data). It performs real traffic analytics using:

  - rdd.map()          → Transform rows into key-value pairs
  - rdd.filter()       → Filter trips by geography, speed, time
  - rdd.reduceByKey()  → Aggregate speeds/counts by cell, hour
  - rdd.groupByKey()   → Group trips by zone
  - rdd.flatMap()      → Explode trip paths into segments
  - rdd.sortByKey()    → Rank congested cells
  - rdd.mapValues()    → Compute averages from (sum, count) pairs
  - rdd.countByKey()   → Count trips per cell
  - rdd.persist()      → Cache intermediate RDDs
  - broadcast variable → Broadcast Manhattan bounds to all workers

Dataset: Cleaned NYC Taxi Parquet files (~7+ GB, 46M+ trips)

Usage:
    python src/batch/traffic_rdd_analysis.py
    python src/batch/traffic_rdd_analysis.py --hdfs

Then open Spark UI: http://localhost:4040
============================================================
"""

import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark import StorageLevel

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from config.spark_config import (
    create_spark_session, HDFS_CONFIG, NYC_BOUNDS, MANHATTAN_BOUNDS
)

# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR = PROJECT_ROOT / "data" / "rdd_output"
HDFS_NAMENODE = HDFS_CONFIG["namenode"]
HDFS_PROCESSED_DIR = HDFS_CONFIG["processed_dir"]

# Congestion speed thresholds (mph)
SPEED_HIGH_CONGESTION = 10    # < 10 mph = High congestion
SPEED_MEDIUM_CONGESTION = 20  # 10-20 mph = Medium congestion
# > 20 mph = Low congestion


def load_traffic_rdd(spark, use_hdfs=False):
    """
    Load cleaned taxi trip data as an RDD from Parquet files.
    Returns an RDD of Row objects with columns like:
      pickup_lat, pickup_lon, speed_mph, trip_distance, hour,
      day_of_week, cell_id, cell_lat, cell_lon, etc.
    """
    print("\n" + "=" * 70)
    print("LOADING REAL TRAFFIC DATA AS RDD")
    print("=" * 70)

    if use_hdfs:
        parquet_path = f"{HDFS_NAMENODE}{HDFS_PROCESSED_DIR}/*_clean.parquet"
        print(f"  Source: HDFS → {parquet_path}")
    else:
        parquet_path = str(DATA_DIR / "*_clean.parquet")
        print(f"  Source: Local → {parquet_path}")

    # Load as DataFrame first, then convert to RDD
    df = spark.read.parquet(parquet_path)
    record_count = df.count()
    print(f"  ✓ Loaded {record_count:,} trip records")

    # Show schema so we know what columns are available
    print("\n  Schema:")
    for field in df.schema.fields:
        print(f"    - {field.name}: {field.dataType.simpleString()}")

    # Convert DataFrame → RDD of Row objects
    rdd = df.rdd
    print(f"\n  ✓ Converted to RDD")
    print(f"    Partitions: {rdd.getNumPartitions()}")
    print(f"    First row type: {type(rdd.first())}")

    return rdd, record_count


# =============================================================================
# RDD ANALYSIS 1: Average Speed by Hour (map + reduceByKey + sortByKey)
# =============================================================================

def analysis_avg_speed_by_hour(rdd):
    """
    Compute average speed per hour of day using RDD API:
      1. map() each row to (hour, (speed, 1))
      2. reduceByKey() to sum speeds and counts
      3. mapValues() to compute average
      4. sortByKey() to order by hour
    """
    print("\n" + "=" * 70)
    print("RDD ANALYSIS 1: Average Speed by Hour of Day")
    print("  Operations: map → filter → reduceByKey → mapValues → sortByKey")
    print("=" * 70)

    start = time.time()

    hourly_avg = rdd \
        .filter(lambda row: row['speed_mph'] is not None and row['speed_mph'] > 0) \
        .map(lambda row: (int(row['hour']), (float(row['speed_mph']), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: round(v[0] / v[1], 2)) \
        .sortByKey() \
        .collect()

    elapsed = time.time() - start

    print(f"\n  Completed in {elapsed:.2f}s\n")
    print("  Hour | Avg Speed (mph) | Congestion")
    print("  -----|-----------------|------------")
    for hour, avg_speed in hourly_avg:
        if avg_speed < SPEED_HIGH_CONGESTION:
            level = "🔴 HIGH"
        elif avg_speed < SPEED_MEDIUM_CONGESTION:
            level = "🟡 MEDIUM"
        else:
            level = "🟢 LOW"
        bar = "█" * int(avg_speed)
        print(f"  {hour:02d}   | {avg_speed:6.2f}          | {level}  {bar}")

    return hourly_avg


# =============================================================================
# RDD ANALYSIS 2: Trip Count per Grid Cell (map + countByKey)
# =============================================================================

def analysis_trips_per_cell(rdd):
    """
    Count trips per grid cell using RDD API:
      1. map() each row to (cell_id, 1)
      2. countByKey() for per-cell counts
      3. Sort results to find busiest cells
    """
    print("\n" + "=" * 70)
    print("RDD ANALYSIS 2: Trip Count per Grid Cell (Top 20 Busiest)")
    print("  Operations: map → countByKey → sorted")
    print("=" * 70)

    start = time.time()

    cell_counts = rdd \
        .map(lambda row: (str(row['cell_id']), 1)) \
        .countByKey()

    elapsed = time.time() - start

    # Sort by count descending
    sorted_cells = sorted(cell_counts.items(), key=lambda x: x[1], reverse=True)

    print(f"\n  Total unique cells: {len(sorted_cells)}")
    print(f"  Completed in {elapsed:.2f}s\n")
    print("  Rank | Cell ID                | Trip Count")
    print("  -----|------------------------|----------")
    for rank, (cell_id, count) in enumerate(sorted_cells[:20], 1):
        print(f"  {rank:4d} | {cell_id:22s} | {count:,}")

    return sorted_cells


# =============================================================================
# RDD ANALYSIS 3: Manhattan vs Outer Boroughs (filter + map + reduce)
# =============================================================================

def analysis_manhattan_vs_outer(rdd, spark_context):
    """
    Compare traffic between Manhattan and outer boroughs:
      1. broadcast() Manhattan geographic bounds to all workers
      2. map() + filter() to separate Manhattan vs outer trips
      3. reduceByKey() to aggregate stats for each zone
    """
    print("\n" + "=" * 70)
    print("RDD ANALYSIS 3: Manhattan vs Outer Boroughs")
    print("  Operations: broadcast → filter → map → reduceByKey → mapValues")
    print("=" * 70)

    start = time.time()

    # Broadcast Manhattan bounds to all worker nodes
    manhattan_bc = spark_context.broadcast(MANHATTAN_BOUNDS)

    def classify_zone(row):
        """Classify a trip as Manhattan or Outer using broadcast variable."""
        bounds = manhattan_bc.value
        lat = float(row['pickup_lat'])
        lon = float(row['pickup_lon'])
        if (bounds['lat_min'] <= lat <= bounds['lat_max'] and
                bounds['lon_min'] <= lon <= bounds['lon_max']):
            return 'Manhattan'
        return 'Outer Boroughs'

    zone_stats = rdd \
        .filter(lambda row: row['speed_mph'] is not None and row['speed_mph'] > 0) \
        .map(lambda row: (
            classify_zone(row),
            (float(row['speed_mph']), float(row['trip_distance']), 1)
        )) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])) \
        .mapValues(lambda v: {
            'avg_speed': round(v[0] / v[2], 2),
            'avg_distance': round(v[1] / v[2], 2),
            'trip_count': v[2]
        }) \
        .collect()

    elapsed = time.time() - start

    print(f"\n  Completed in {elapsed:.2f}s\n")
    print("  Zone             | Trips       | Avg Speed | Avg Distance")
    print("  -----------------|-------------|-----------|-------------")
    for zone, stats in zone_stats:
        print(f"  {zone:17s}| {stats['trip_count']:>11,} | "
              f"{stats['avg_speed']:>6.2f} mph | {stats['avg_distance']:>5.2f} mi")

    return zone_stats


# =============================================================================
# RDD ANALYSIS 4: Congestion Hotspot Detection (filter + map + reduceByKey)
# =============================================================================

def analysis_congestion_hotspots(rdd):
    """
    Detect congested hotspots during rush hours:
      1. filter() rush hour trips (7-9 AM, 5-7 PM) with low speed
      2. map() to (cell_id, (speed, count))
      3. reduceByKey() to aggregate per cell
      4. filter() cells with high congestion
      5. sortBy() congestion severity
    """
    print("\n" + "=" * 70)
    print("RDD ANALYSIS 4: Rush Hour Congestion Hotspots")
    print("  Operations: filter → map → reduceByKey → mapValues → filter → sortBy")
    print("=" * 70)

    start = time.time()

    RUSH_HOURS = {7, 8, 9, 17, 18, 19}

    hotspots = rdd \
        .filter(lambda row: (
            row['hour'] is not None and
            int(row['hour']) in RUSH_HOURS and
            row['speed_mph'] is not None and
            row['speed_mph'] > 0
        )) \
        .map(lambda row: (
            str(row['cell_id']),
            (float(row['speed_mph']), 1)
        )) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: {
            'avg_speed': round(v[0] / v[1], 2),
            'trip_count': v[1]
        }) \
        .filter(lambda x: x[1]['avg_speed'] < SPEED_MEDIUM_CONGESTION and x[1]['trip_count'] >= 10) \
        .sortBy(lambda x: x[1]['avg_speed'], ascending=True) \
        .collect()

    elapsed = time.time() - start

    print(f"\n  Rush hour hotspots (speed < {SPEED_MEDIUM_CONGESTION} mph): {len(hotspots)}")
    print(f"  Completed in {elapsed:.2f}s\n")
    print("  Rank | Cell ID                | Avg Speed | Trips  | Severity")
    print("  -----|------------------------|-----------|--------|----------")
    for rank, (cell_id, stats) in enumerate(hotspots[:25], 1):
        severity = "🔴 CRITICAL" if stats['avg_speed'] < 8 else "🟠 HIGH" if stats['avg_speed'] < 12 else "🟡 MODERATE"
        print(f"  {rank:4d} | {cell_id:22s} | {stats['avg_speed']:6.2f}    | {stats['trip_count']:>6,} | {severity}")

    return hotspots


# =============================================================================
# RDD ANALYSIS 5: Speed Distribution using flatMap (flatMap + map + reduceByKey)
# =============================================================================

def analysis_speed_distribution(rdd):
    """
    Build a speed histogram using RDD API:
      1. flatMap() each row to speed bucket labels
      2. map() to (bucket, 1)
      3. reduceByKey() to count per bucket
    """
    print("\n" + "=" * 70)
    print("RDD ANALYSIS 5: Speed Distribution Histogram")
    print("  Operations: filter → flatMap → reduceByKey")
    print("=" * 70)

    start = time.time()

    def speed_to_buckets(row):
        """Assign a trip to one or more speed buckets."""
        speed = float(row['speed_mph'])
        buckets = []
        if speed < 5:
            buckets.append("00-05 mph (Crawling)")
        elif speed < 10:
            buckets.append("05-10 mph (Very Slow)")
        elif speed < 15:
            buckets.append("10-15 mph (Slow)")
        elif speed < 20:
            buckets.append("15-20 mph (Moderate)")
        elif speed < 30:
            buckets.append("20-30 mph (Normal)")
        elif speed < 40:
            buckets.append("30-40 mph (Fast)")
        else:
            buckets.append("40+   mph (Highway)")
        return buckets

    distribution = rdd \
        .filter(lambda row: row['speed_mph'] is not None and row['speed_mph'] > 0) \
        .flatMap(lambda row: [(bucket, 1) for bucket in speed_to_buckets(row)]) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()

    elapsed = time.time() - start

    # Sort by bucket name
    distribution = sorted(distribution, key=lambda x: x[0])
    total = sum(count for _, count in distribution)

    print(f"\n  Total trips analyzed: {total:,}")
    print(f"  Completed in {elapsed:.2f}s\n")
    print("  Speed Bucket           | Count       | Pct    | Histogram")
    print("  -----------------------|-------------|--------|------------------")
    for bucket, count in distribution:
        pct = (count / total) * 100
        bar = "█" * int(pct / 2)
        print(f"  {bucket:23s}| {count:>11,} | {pct:5.1f}% | {bar}")

    return distribution


# =============================================================================
# RDD ANALYSIS 6: Day-of-Week Pattern (groupByKey + mapValues)
# =============================================================================

def analysis_day_of_week_pattern(rdd):
    """
    Analyze traffic patterns by day of week:
      1. map() to (day_of_week, speed)
      2. groupByKey() to collect all speeds per day
      3. mapValues() to compute stats (mean, min, max, count)
    """
    print("\n" + "=" * 70)
    print("RDD ANALYSIS 6: Traffic Pattern by Day of Week")
    print("  Operations: map → groupByKey → mapValues → sortByKey")
    print("=" * 70)

    start = time.time()

    DAY_NAMES = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday',
                 4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'}

    day_stats = rdd \
        .filter(lambda row: row['speed_mph'] is not None and row['speed_mph'] > 0) \
        .map(lambda row: (int(row['day_of_week']), float(row['speed_mph']))) \
        .groupByKey() \
        .mapValues(lambda speeds: {
            'avg_speed': round(sum(speeds) / len(speeds), 2),
            'min_speed': round(min(speeds), 2),
            'max_speed': round(max(speeds), 2),
            'trip_count': len(speeds)
        }) \
        .sortByKey() \
        .collect()

    elapsed = time.time() - start

    print(f"\n  Completed in {elapsed:.2f}s\n")
    print("  Day        | Trips       | Avg Speed | Min   | Max    | Congestion")
    print("  -----------|-------------|-----------|-------|--------|----------")
    for day_num, stats in day_stats:
        day_name = DAY_NAMES.get(day_num, f"Day {day_num}")
        level = "🔴 High" if stats['avg_speed'] < 12 else "🟡 Med" if stats['avg_speed'] < 18 else "🟢 Low"
        print(f"  {day_name:10s} | {stats['trip_count']:>11,} | "
              f"{stats['avg_speed']:6.2f}    | {stats['min_speed']:5.1f} | {stats['max_speed']:6.1f} | {level}")

    return day_stats


# =============================================================================
# RDD CACHING / PERSISTENCE DEMO
# =============================================================================

def demonstrate_caching(rdd):
    """
    Show performance difference between cached and uncached RDD.
    """
    print("\n" + "=" * 70)
    print("RDD CACHING PERFORMANCE COMPARISON")
    print("=" * 70)

    # Prepare a key-value RDD
    kv_rdd = rdd \
        .filter(lambda row: row['speed_mph'] is not None and row['speed_mph'] > 0) \
        .map(lambda row: (int(row['hour']), float(row['speed_mph'])))

    # ---- UNCACHED ----
    print("\n  🔵 Uncached: Running reduceByKey twice (recomputes from disk each time)")
    start = time.time()
    result1 = kv_rdd \
        .mapValues(lambda s: (s, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()
    time_uncached_1 = time.time() - start

    start = time.time()
    result2 = kv_rdd \
        .mapValues(lambda s: (s, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()
    time_uncached_2 = time.time() - start

    print(f"    Run 1: {time_uncached_1:.3f}s")
    print(f"    Run 2: {time_uncached_2:.3f}s")

    # ---- CACHED ----
    print("\n  🟢 Cached: Persisting in MEMORY_AND_DISK, running twice")
    kv_rdd_cached = kv_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    kv_rdd_cached.setName("TrafficRDD_HourSpeed_CACHED")

    start = time.time()
    result3 = kv_rdd_cached \
        .mapValues(lambda s: (s, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()
    time_cached_1 = time.time() - start  # Cold cache (first materialization)

    start = time.time()
    result4 = kv_rdd_cached \
        .mapValues(lambda s: (s, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .collect()
    time_cached_2 = time.time() - start  # Warm cache

    print(f"    Run 1 (cold cache): {time_cached_1:.3f}s")
    print(f"    Run 2 (warm cache): {time_cached_2:.3f}s")

    if time_cached_2 > 0:
        speedup = time_uncached_2 / time_cached_2
        print(f"\n  ✓ Speedup (warm cache vs uncached): {speedup:.2f}x")

    # Check storage in Spark UI
    print(f"\n  🌐 Check cached RDD in Spark UI → http://localhost:4040/storage/")

    # Cleanup
    kv_rdd_cached.unpersist()

    return {
        'uncached_1': time_uncached_1,
        'uncached_2': time_uncached_2,
        'cached_cold': time_cached_1,
        'cached_warm': time_cached_2,
    }


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='RDD Analysis on Real Traffic Data')
    parser.add_argument('--local', action='store_true', help='Run in local mode instead of cluster')
    parser.add_argument('--no-hdfs', action='store_true', help='Use local filesystem instead of HDFS')
    args = parser.parse_args()
    args.hdfs = not args.no_hdfs  # Default: HDFS ON

    print("""
╔══════════════════════════════════════════════════════════════════════╗
║   SMART CITY TRAFFIC - RDD API ANALYSIS ON REAL TRAFFIC DATA       ║
║                                                                      ║
║   This script uses the RDD API (map, filter, reduceByKey, etc.)     ║
║   on actual NYC taxi trip data from cleaned Parquet files.           ║
╚══════════════════════════════════════════════════════════════════════╝
""")

    overall_start = datetime.now()

    # Create Spark session
    spark = create_spark_session(
        app_name="SmartCityTraffic-RDD-Analysis",
        use_hdfs=args.hdfs,
        driver_memory="4g"
    )
    sc = spark.sparkContext

    print(f"\n🌐 Spark UI available at: http://localhost:4040")

    try:
        # Load data as RDD
        rdd, total_records = load_traffic_rdd(spark, use_hdfs=args.hdfs)

        # Analysis 1: Average Speed by Hour
        analysis_avg_speed_by_hour(rdd)

        # Analysis 2: Trip Count per Cell
        analysis_trips_per_cell(rdd)

        # Analysis 3: Manhattan vs Outer Boroughs (uses broadcast)
        analysis_manhattan_vs_outer(rdd, sc)

        # Analysis 4: Rush Hour Congestion Hotspots
        analysis_congestion_hotspots(rdd)

        # Analysis 5: Speed Distribution (uses flatMap)
        analysis_speed_distribution(rdd)

        # Analysis 6: Day-of-Week Pattern (uses groupByKey)
        analysis_day_of_week_pattern(rdd)

        # Caching Performance Demo
        demonstrate_caching(rdd)

        # Final Summary
        elapsed = (datetime.now() - overall_start).total_seconds()
        print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║   ✅  ALL RDD ANALYSES COMPLETE                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║   Total Records Analyzed: {total_records:>12,}                          ║
║   Total Time:             {elapsed:>10.1f}s                            ║
║                                                                      ║
║   RDD Operations Used:                                               ║
║     ✓ map()          - Transform rows to key-value pairs             ║
║     ✓ filter()       - Filter by speed, hour, geography              ║
║     ✓ reduceByKey()  - Aggregate speeds & counts per key             ║
║     ✓ groupByKey()   - Collect all values per key                    ║
║     ✓ flatMap()      - Map rows to multiple speed buckets            ║
║     ✓ mapValues()    - Compute averages from (sum, count)            ║
║     ✓ sortByKey()    - Order results by key                          ║
║     ✓ sortBy()       - Custom sort by congestion severity            ║
║     ✓ countByKey()   - Count trips per cell                          ║
║     ✓ persist()      - Cache RDDs in MEMORY_AND_DISK                 ║
║     ✓ broadcast()    - Broadcast Manhattan bounds to workers         ║
║     ✓ collect()      - Retrieve results to driver                    ║
╚══════════════════════════════════════════════════════════════════════╝
""")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        print("✓ Spark session stopped.\n")


if __name__ == "__main__":
    main()
