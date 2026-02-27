import os
import sys
from pathlib import Path
from datetime import datetime
import time

from pyspark.sql import SparkSession
from pyspark import StorageLevel

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR = PROJECT_ROOT / "data" / "rdd_output"

# NYC Geographic bounds
NYC_LAT_MIN = 40.4774
NYC_LAT_MAX = 40.9176
NYC_LON_MIN = -74.2591
NYC_LON_MAX = -73.7004


def create_spark_session():
    """
    Create Spark session with UI enabled.
    Spark UI will be available at http://localhost:4040
    """
    print("\n" + "="*70)
    print("CREATING SPARK SESSION WITH UI ENABLED")
    print("="*70)
    
    # Windows workaround for Hadoop
    if os.name == 'nt':
        hadoop_home = r"C:\hadoop"
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ['hadoop.home.dir'] = hadoop_home
    
    spark = SparkSession.builder \
        .appName("TrafficRDD-Optimization") \
        .master("local[*]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "8") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"\n✓ Spark Session Created!")
    print(f"  App Name: {spark.sparkContext.appName}")
    print(f"  Spark Version: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"\n🌐 Spark UI: http://localhost:4040")
    print(f"  Navigate to 'Storage' tab to view cached RDDs")
    print("="*70)
    
    return spark


def load_data_as_rdd(spark):
    """
    Load processed traffic data and convert to RDD.
    """
    print("\n" + "="*70)
    print("STEP 1: LOADING DATA AS RDD")
    print("="*70)
    
    # Find processed parquet files
    parquet_files = list(DATA_DIR.glob("yellow_tripdata_*_clean.parquet"))
    
    if not parquet_files:
        print("❌ No processed data found!")
        print(f"  Run data cleaning first: python src/batch/data_cleaning_spark.py")
        return None
    
    print(f"\nFound {len(parquet_files)} processed files")
    
    # Load first file as DataFrame, then convert to RDD
    parquet_path = str(parquet_files[0])
    print(f"Loading: {parquet_path}")
    
    df = spark.read.parquet(parquet_path)
    
    # Convert to RDD
    rdd = df.rdd
    
    print(f"\n✓ Created RDD from DataFrame")
    print(f"  Partitions: {rdd.getNumPartitions()}")
    print(f"  First element type: {type(rdd.first())}")
    
    # Show sample data
    print("\n📊 Sample Records (first 3):")
    for i, row in enumerate(rdd.take(3), 1):
        print(f"  {i}. Hour: {row['hour']}, Speed: {row['speed_mph']:.1f} mph, "
              f"Distance: {row['trip_distance']:.2f} mi")
    
    return rdd


def demonstrate_storage_levels(spark, base_rdd):
    """
    Demonstrate different RDD storage levels and their impact.
    """
    print("\n" + "="*70)
    print("STEP 2: DEMONSTRATING STORAGE LEVELS")
    print("="*70)
    
    print("\n📚 Available Storage Levels:")
    print("  1. MEMORY_ONLY - Store in memory only (default)")
    print("  2. MEMORY_AND_DISK - Spill to disk if memory full")
    print("  3. MEMORY_ONLY_SER - Serialize objects in memory (more compact)")
    print("  4. MEMORY_AND_DISK_SER - Serialize + disk spillage")
    print("  5. DISK_ONLY - Store only on disk")
    print("  6. MEMORY_ONLY_2 - Replicate each partition on 2 nodes")
    print("  7. OFF_HEAP - Store in off-heap memory")
    
    # Create multiple RDDs with different storage strategies
    rdds = {}
    
    # 1. MEMORY_ONLY
    print("\n🔹 Creating RDD with MEMORY_ONLY storage...")
    rdd_memory = base_rdd \
        .map(lambda row: (row['hour'], row['speed_mph'])) \
        .filter(lambda x: x[1] is not None)
    
    rdd_memory.persist(StorageLevel.MEMORY_ONLY)
    rdd_memory.setName("Traffic_MEMORY_ONLY")
    count_1 = rdd_memory.count()  # Trigger caching
    print(f"  ✓ Cached {count_1:,} records in MEMORY_ONLY")
    print(f"  Storage Level: {rdd_memory.getStorageLevel()}")
    rdds['MEMORY_ONLY'] = rdd_memory
    
    # 2. MEMORY_AND_DISK
    print("\n🔹 Creating RDD with MEMORY_AND_DISK storage...")
    rdd_mem_disk = base_rdd \
        .map(lambda row: (row['cell_id'], (row['speed_mph'], row['trip_distance']))) \
        .filter(lambda x: x[1][0] is not None)
    
    rdd_mem_disk.persist(StorageLevel.MEMORY_AND_DISK)
    rdd_mem_disk.setName("Traffic_MEMORY_AND_DISK")
    count_2 = rdd_mem_disk.count()  # Trigger caching
    print(f"  ✓ Cached {count_2:,} records in MEMORY_AND_DISK")
    print(f"  Storage Level: {rdd_mem_disk.getStorageLevel()}")
    rdds['MEMORY_AND_DISK'] = rdd_mem_disk
    
    # 3. MEMORY_ONLY_SER (Serialized)
    print("\n🔹 Creating RDD with MEMORY_ONLY_SER storage...")
    rdd_ser = base_rdd \
        .map(lambda row: (row['day_of_week'], row['hour'], row['speed_mph'])) \
        .filter(lambda x: x[2] is not None)
    
    rdd_ser.persist(StorageLevel.MEMORY_ONLY_SER)
    rdd_ser.setName("Traffic_MEMORY_ONLY_SER")
    count_3 = rdd_ser.count()  # Trigger caching
    print(f"  ✓ Cached {count_3:,} records in MEMORY_ONLY_SER")
    print(f"  Storage Level: {rdd_ser.getStorageLevel()}")
    rdds['MEMORY_ONLY_SER'] = rdd_ser
    
    # 4. DISK_ONLY
    print("\n🔹 Creating RDD with DISK_ONLY storage...")
    rdd_disk = base_rdd \
        .map(lambda row: (row['cell_lat'], row['cell_lon'], row['speed_mph'])) \
        .filter(lambda x: x[2] is not None)
    
    rdd_disk.persist(StorageLevel.DISK_ONLY)
    rdd_disk.setName("Traffic_DISK_ONLY")
    count_4 = rdd_disk.count()  # Trigger caching
    print(f"  ✓ Cached {count_4:,} records in DISK_ONLY")
    print(f"  Storage Level: {rdd_disk.getStorageLevel()}")
    rdds['DISK_ONLY'] = rdd_disk
    
    print("\n" + "="*70)
    print("✓ ALL RDDs CACHED - Check Spark UI Storage Tab!")
    print("="*70)
    print(f"\n🌐 Open: http://localhost:4040/storage/")
    print("  You should see 4 RDDs in the Storage tab:")
    print("    1. Traffic_MEMORY_ONLY")
    print("    2. Traffic_MEMORY_AND_DISK")
    print("    3. Traffic_MEMORY_ONLY_SER")
    print("    4. Traffic_DISK_ONLY")
    
    return rdds


def optimization_techniques(base_rdd):
    """
    Demonstrate RDD optimization techniques.
    """
    print("\n" + "="*70)
    print("STEP 3: RDD OPTIMIZATION TECHNIQUES")
    print("="*70)
    
    # Technique 1: Repartitioning
    print("\n🔧 OPTIMIZATION 1: Repartitioning")
    print(f"  Original partitions: {base_rdd.getNumPartitions()}")
    
    # Repartition to optimal number (2x number of cores)
    optimal_partitions = 8
    rdd_repartitioned = base_rdd.repartition(optimal_partitions)
    print(f"  After repartition: {rdd_repartitioned.getNumPartitions()}")
    print(f"  ✓ Better parallelism for computation")
    
    # Technique 2: Coalesce (reduce partitions efficiently)
    print("\n🔧 OPTIMIZATION 2: Coalesce")
    rdd_coalesced = rdd_repartitioned.coalesce(4)
    print(f"  After coalesce: {rdd_coalesced.getNumPartitions()}")
    print(f"  ✓ Reduced partitions without full shuffle")
    
    # Technique 3: Filter early (predicate pushdown)
    print("\n🔧 OPTIMIZATION 3: Early Filtering")
    start_time = time.time()
    
    # Bad: Filter late
    bad_rdd = base_rdd \
        .map(lambda row: (row['hour'], row['speed_mph'], row['trip_distance'])) \
        .map(lambda x: (x[0], x[1] * 1.6))  # Convert to km/h \
        .filter(lambda x: x[1] < 15)  # Filter AFTER transformations
    
    bad_count = bad_rdd.count()
    bad_time = time.time() - start_time
    
    # Good: Filter early
    start_time = time.time()
    good_rdd = base_rdd \
        .filter(lambda row: row['speed_mph'] < 15)  # Filter FIRST \
        .map(lambda row: (row['hour'], row['speed_mph'])) \
        .map(lambda x: (x[0], x[1] * 1.6))
    
    good_count = good_rdd.count()
    good_time = time.time() - start_time
    
    print(f"  Without early filter: {bad_time:.3f}s for {bad_count:,} records")
    print(f"  With early filter: {good_time:.3f}s for {good_count:,} records")
    print(f"  ✓ Speedup: {bad_time/good_time:.2f}x faster!")
    
    # Technique 4: Avoiding GroupByKey (use ReduceByKey instead)
    print("\n🔧 OPTIMIZATION 4: ReduceByKey vs GroupByKey")
    
    # Create key-value RDD
    kv_rdd = base_rdd.map(lambda row: (row['hour'], row['speed_mph']))
    
    # Bad: GroupByKey (shuffles all values)
    start_time = time.time()
    bad_result = kv_rdd.groupByKey().mapValues(lambda speeds: sum(speeds) / len(speeds))
    bad_result.count()
    bad_time = time.time() - start_time
    
    # Good: ReduceByKey (combines before shuffling)
    start_time = time.time()
    good_result = kv_rdd \
        .mapValues(lambda speed: (speed, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda x: x[0] / x[1])
    good_result.count()
    good_time = time.time() - start_time
    
    print(f"  GroupByKey: {bad_time:.3f}s")
    print(f"  ReduceByKey: {good_time:.3f}s")
    print(f"  ✓ Speedup: {bad_time/good_time:.2f}x faster!")
    
    # Technique 5: Broadcasting small data
    print("\n🔧 OPTIMIZATION 5: Broadcasting")
    
    # Small lookup table
    manhattan_bounds = {
        'lat_min': 40.70, 'lat_max': 40.88,
        'lon_min': -74.02, 'lon_max': -73.93
    }
    
    # Broadcast to all workers
    broadcast_bounds = base_rdd.context.broadcast(manhattan_bounds)
    
    def is_manhattan(lat, lon):
        bounds = broadcast_bounds.value
        return (bounds['lat_min'] <= lat <= bounds['lat_max'] and
                bounds['lon_min'] <= lon <= bounds['lon_max'])
    
    manhattan_rdd = base_rdd \
        .filter(lambda row: is_manhattan(row['pickup_lat'], row['pickup_lon']))
    
    manhattan_count = manhattan_rdd.count()
    print(f"  ✓ Broadcast lookup table to all workers")
    print(f"  Manhattan trips: {manhattan_count:,}")
    
    return {
        'repartitioned': rdd_repartitioned,
        'coalesced': rdd_coalesced,
        'filtered_early': good_rdd
    }


def performance_comparison(rdds):
    """
    Compare performance of cached vs uncached RDDs.
    """
    print("\n" + "="*70)
    print("STEP 4: PERFORMANCE COMPARISON (CACHED vs UNCACHED)")
    print("="*70)
    
    if 'MEMORY_ONLY' not in rdds:
        print("⚠ Cached RDDs not available")
        return
    
    cached_rdd = rdds['MEMORY_ONLY']
    
    # Create uncached version
    uncached_rdd = cached_rdd.unpersist()
    uncached_rdd = cached_rdd.map(lambda x: x)  # Create new lineage
    
    # Test 1: Count operation
    print("\n📊 Test 1: Count Operation")
    
    # Uncached (first run)
    start = time.time()
    count1 = uncached_rdd.count()
    time1 = time.time() - start
    print(f"  Uncached (cold): {time1:.3f}s")
    
    # Cached (first run - loads into cache)
    cached_rdd.persist(StorageLevel.MEMORY_ONLY)
    start = time.time()
    count2 = cached_rdd.count()
    time2 = time.time() - start
    print(f"  Cached (cold): {time2:.3f}s")
    
    # Cached (second run - reads from cache)
    start = time.time()
    count3 = cached_rdd.count()
    time3 = time.time() - start
    print(f"  Cached (warm): {time3:.3f}s")
    print(f"  ✓ Speedup (warm): {time1/time3:.2f}x faster!")
    
    # Test 2: Complex operation
    print("\n📊 Test 2: Aggregation Operation")
    
    # Uncached
    start = time.time()
    result1 = uncached_rdd \
        .map(lambda x: (x[0], x[1])) \
        .groupByKey() \
        .mapValues(lambda speeds: sum(speeds) / len(speeds)) \
        .collect()
    time1 = time.time() - start
    print(f"  Uncached: {time1:.3f}s")
    
    # Cached
    start = time.time()
    result2 = cached_rdd \
        .map(lambda x: (x[0], x[1])) \
        .groupByKey() \
        .mapValues(lambda speeds: sum(speeds) / len(speeds)) \
        .collect()
    time2 = time.time() - start
    print(f"  Cached: {time2:.3f}s")
    print(f"  ✓ Speedup: {time1/time2:.2f}x faster!")


def analyze_traffic_with_rdd(rdds):
    """
    Perform traffic analysis using cached RDDs.
    """
    print("\n" + "="*70)
    print("STEP 5: TRAFFIC ANALYSIS WITH OPTIMIZED RDDs")
    print("="*70)
    
    if 'MEMORY_ONLY' not in rdds:
        print("⚠ Cached RDDs not available")
        return
    
    rdd = rdds['MEMORY_ONLY']
    
    # Analysis 1: Average speed by hour
    print("\n📊 Analysis 1: Average Speed by Hour")
    hourly_stats = rdd \
        .map(lambda x: (x[0], (x[1], 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda x: x[0] / x[1]) \
        .sortByKey() \
        .collect()
    
    print("\n  Hour | Avg Speed | Congestion Level")
    print("  -----|-----------|------------------")
    for hour, avg_speed in hourly_stats:
        congestion = "🔴 High" if avg_speed < 10 else "🟡 Medium" if avg_speed < 20 else "🟢 Low"
        print(f"  {hour:02d}   | {avg_speed:5.1f} mph | {congestion}")
    
    # Analysis 2: Rush hour detection
    print("\n📊 Analysis 2: Rush Hour Detection")
    rush_hours = [(h, s) for h, s in hourly_stats if s < 15]
    print(f"\n  Detected {len(rush_hours)} rush hours (speed < 15 mph):")
    for hour, speed in rush_hours:
        print(f"    • Hour {hour:02d}: {speed:.1f} mph")
    
    # Analysis 3: Speed distribution
    print("\n📊 Analysis 3: Speed Distribution")
    speed_ranges = rdd \
        .map(lambda x: x[1]) \
        .map(lambda speed: 
             "0-10 mph" if speed < 10 else
             "10-20 mph" if speed < 20 else
             "20-30 mph" if speed < 30 else
             "30+ mph") \
        .map(lambda range: (range, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    
    total = sum(count for _, count in speed_ranges)
    print("\n  Speed Range | Count      | Percentage")
    print("  ------------|------------|------------")
    for range_name, count in sorted(speed_ranges):
        pct = (count / total) * 100
        bar = "█" * int(pct / 2)
        print(f"  {range_name:11} | {count:10,} | {pct:5.1f}% {bar}")


def view_storage_stats(spark):
    """
    Display storage statistics from Spark context.
    """
    print("\n" + "="*70)
    print("STEP 6: STORAGE STATISTICS")
    print("="*70)
    
    sc = spark.sparkContext
    status_tracker = sc.statusTracker()
    
    # Get RDD info
    rdd_infos = sc._jsc.sc().getRDDStorageInfo()
    
    print(f"\n📊 Total RDDs in Storage: {len(rdd_infos)}")
    
    if len(rdd_infos) > 0:
        print("\n  RDD Details:")
        for i, rdd_info in enumerate(rdd_infos, 1):
            name = rdd_info.name()
            partitions = rdd_info.numPartitions()
            cached_partitions = rdd_info.numCachedPartitions()
            memory_size = rdd_info.memSize() / (1024 * 1024)  # MB
            disk_size = rdd_info.diskSize() / (1024 * 1024)   # MB
            storage_level = rdd_info.storageLevel().description()
            
            print(f"\n  {i}. {name}")
            print(f"     Partitions: {cached_partitions}/{partitions} cached")
            print(f"     Memory: {memory_size:.2f} MB")
            print(f"     Disk: {disk_size:.2f} MB")
            print(f"     Storage Level: {storage_level}")
    
    print("\n" + "="*70)
    print("🌐 For detailed visualization, check Spark UI:")
    print("   http://localhost:4040/storage/")
    print("="*70)


def cleanup_rdds(rdds):
    """
    Unpersist all cached RDDs to free memory.
    """
    print("\n" + "="*70)
    print("CLEANUP: Unpersisting Cached RDDs")
    print("="*70)
    
    for name, rdd in rdds.items():
        rdd.unpersist()
        print(f"  ✓ Unpersisted: {name}")
    
    print("\n✓ All RDDs unpersisted - memory freed!")


def main():
    """
    Main execution function.
    """
    print("\n" + "="*70)
    print("SMART CITY TRAFFIC - RDD OPTIMIZATION & STORAGE DEMO")
    print("="*70)
    print("\nThis demo will:")
    print("  1. Create RDDs from traffic data")
    print("  2. Apply different storage levels")
    print("  3. Demonstrate optimization techniques")
    print("  4. Compare cached vs uncached performance")
    print("  5. Show storage status in Spark UI")
    
    start_time = datetime.now()
    
    # Create Spark session
    spark = create_spark_session()
    
    print("\n⏳ Waiting 5 seconds for Spark UI to initialize...")
    print("   Open http://localhost:4040 in your browser now!")
    time.sleep(5)
    
    try:
        # Step 1: Load data as RDD
        base_rdd = load_data_as_rdd(spark)
        
        if base_rdd is None:
            print("\n❌ Cannot proceed without data!")
            return
        
        # Step 2: Demonstrate storage levels
        input("\n⏸️  Press ENTER to continue to storage level demo...")
        cached_rdds = demonstrate_storage_levels(spark, base_rdd)
        
        # Step 3: Optimization techniques
        input("\n⏸️  Press ENTER to continue to optimization techniques...")
        optimized_rdds = optimization_techniques(base_rdd)
        
        # Step 4: Performance comparison
        input("\n⏸️  Press ENTER to continue to performance comparison...")
        performance_comparison(cached_rdds)
        
        # Step 5: Traffic analysis
        input("\n⏸️  Press ENTER to continue to traffic analysis...")
        analyze_traffic_with_rdd(cached_rdds)
        
        # Step 6: View storage stats
        input("\n⏸️  Press ENTER to view storage statistics...")
        view_storage_stats(spark)
        
        # Keep UI alive
        print("\n" + "="*70)
        print("✅ DEMO COMPLETE!")
        print("="*70)
        print("\n📊 Spark UI is still running at: http://localhost:4040")
        print("   Navigate to different tabs to explore:")
        print("     • Jobs - See all executed jobs")
        print("     • Stages - See task execution details")
        print("     • Storage - See cached RDDs (4 RDDs cached)")
        print("     • Environment - See Spark configuration")
        print("     • Executors - See executor statistics")
        
        input("\n⏸️  Press ENTER to cleanup and exit...")
        
        # Cleanup
        cleanup_rdds(cached_rdds)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark
        print("\n🛑 Stopping Spark session...")
        spark.stop()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("SESSION COMPLETE")
        print("="*70)
        print(f"  Duration: {duration:.1f} seconds")
        print(f"  Started: {start_time.strftime('%H:%M:%S')}")
        print(f"  Ended: {end_time.strftime('%H:%M:%S')}")
        print("="*70 + "\n")


if __name__ == "__main__":
    main()
