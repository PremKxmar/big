"""
Simple RDD Optimization & Storage Level Demo
==============================================

A standalone example demonstrating:
1. Creating RDDs from simple data
2. Applying different storage levels (MEMORY_ONLY, MEMORY_AND_DISK, etc.)
3. Viewing storage in Spark UI
4. Basic optimization techniques

Usage:
    python simple_rdd_demo.py
    
    Then open: http://localhost:4040/storage/
    
Author: Simple Demo
Date: January 31, 2026
"""

import time
from pyspark.sql import SparkSession
from pyspark import StorageLevel


def create_spark():
    """Create Spark session with UI enabled"""
    print("\n" + "="*60)
    print("Creating Spark Session")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("Simple-RDD-Demo") \
        .master("local[*]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"\n✓ Spark Session Created!")
    print(f"  Version: {spark.version}")
    print(f"  UI: http://localhost:4040")
    print("="*60)
    
    return spark


def create_sample_rdds(spark):
    """Create RDDs from simple data"""
    print("\n" + "="*60)
    print("STEP 1: Creating RDDs")
    print("="*60)
    
    sc = spark.sparkContext
    
    # Create simple numeric RDD
    numbers = range(1, 1000000)
    rdd_numbers = sc.parallelize(numbers, numSlices=4)
    print(f"\n1. Created RDD with {rdd_numbers.count():,} numbers")
    print(f"   Partitions: {rdd_numbers.getNumPartitions()}")
    
    # Create RDD with tuples (key-value pairs)
    data = [(i, i**2) for i in range(1, 100000)]
    rdd_pairs = sc.parallelize(data, numSlices=4)
    print(f"\n2. Created RDD with {rdd_pairs.count():,} key-value pairs")
    print(f"   Sample: {rdd_pairs.take(3)}")
    
    return rdd_numbers, rdd_pairs


def demonstrate_storage_levels(rdd_numbers, rdd_pairs):
    """Apply different storage levels and cache RDDs"""
    print("\n" + "="*60)
    print("STEP 2: Applying Storage Levels")
    print("="*60)
    
    # Storage Level 1: MEMORY_ONLY
    print("\n📦 Storage Level 1: MEMORY_ONLY")
    rdd1 = rdd_numbers.map(lambda x: x * 2)
    rdd1.persist(StorageLevel.MEMORY_ONLY)
    rdd1.setName("RDD_MEMORY_ONLY")
    count1 = rdd1.count()  # Trigger caching
    print(f"   ✓ Cached {count1:,} elements in MEMORY_ONLY")
    print(f"   Storage: {rdd1.getStorageLevel()}")
    
    # Storage Level 2: MEMORY_AND_DISK
    print("\n📦 Storage Level 2: MEMORY_AND_DISK")
    rdd2 = rdd_pairs.map(lambda x: (x[0], x[1] * 2))
    rdd2.persist(StorageLevel.MEMORY_AND_DISK)
    rdd2.setName("RDD_MEMORY_AND_DISK")
    count2 = rdd2.count()  # Trigger caching
    print(f"   ✓ Cached {count2:,} elements in MEMORY_AND_DISK")
    print(f"   Storage: {rdd2.getStorageLevel()}")
    
    # Storage Level 3: MEMORY_ONLY_SER (Serialized)
    print("\n📦 Storage Level 3: MEMORY_ONLY_SER (Serialized)")
    rdd3 = rdd_numbers.filter(lambda x: x % 2 == 0)
    rdd3.persist(StorageLevel.MEMORY_ONLY_SER)
    rdd3.setName("RDD_MEMORY_ONLY_SER")
    count3 = rdd3.count()  # Trigger caching
    print(f"   ✓ Cached {count3:,} elements in MEMORY_ONLY_SER")
    print(f"   Storage: {rdd3.getStorageLevel()}")
    
    # Storage Level 4: DISK_ONLY
    print("\n📦 Storage Level 4: DISK_ONLY")
    rdd4 = rdd_pairs.filter(lambda x: x[0] > 50000)
    rdd4.persist(StorageLevel.DISK_ONLY)
    rdd4.setName("RDD_DISK_ONLY")
    count4 = rdd4.count()  # Trigger caching
    print(f"   ✓ Cached {count4:,} elements in DISK_ONLY")
    print(f"   Storage: {rdd4.getStorageLevel()}")
    
    print("\n" + "="*60)
    print("✓ ALL 4 RDDs ARE NOW CACHED!")
    print("="*60)
    print("\n🌐 CHECK SPARK UI STORAGE TAB:")
    print("   http://localhost:4040/storage/")
    print("\n   You should see 4 cached RDDs:")
    print("   1. RDD_MEMORY_ONLY")
    print("   2. RDD_MEMORY_AND_DISK")
    print("   3. RDD_MEMORY_ONLY_SER")
    print("   4. RDD_DISK_ONLY")
    
    return rdd1, rdd2, rdd3, rdd4


def optimization_demo(rdd_numbers):
    """Demonstrate RDD optimization techniques"""
    print("\n" + "="*60)
    print("STEP 3: Optimization Techniques")
    print("="*60)
    
    # Optimization 1: Filter Early
    print("\n🔧 Optimization 1: Early Filtering")
    
    # Bad: Filter late
    start = time.time()
    bad = rdd_numbers \
        .map(lambda x: x * 2) \
        .map(lambda x: x + 10) \
        .filter(lambda x: x < 1000)  # Filter AFTER transformations
    bad_count = bad.count()
    bad_time = time.time() - start
    
    # Good: Filter early
    start = time.time()
    good = rdd_numbers \
        .filter(lambda x: x < 500) \
        .map(lambda x: x * 2) \
        .map(lambda x: x + 10)
    good_count = good.count()
    good_time = time.time() - start
    
    print(f"   Late filter: {bad_time:.3f}s ({bad_count:,} results)")
    print(f"   Early filter: {good_time:.3f}s ({good_count:,} results)")
    print(f"   ✓ Speedup: {bad_time/good_time:.2f}x faster!")
    
    # Optimization 2: ReduceByKey vs GroupByKey
    print("\n🔧 Optimization 2: ReduceByKey vs GroupByKey")
    
    # Create key-value pairs
    kv_rdd = rdd_numbers.map(lambda x: (x % 100, x))
    
    # Bad: GroupByKey
    start = time.time()
    bad_result = kv_rdd.groupByKey().mapValues(lambda vals: sum(vals))
    bad_result.count()
    bad_time = time.time() - start
    
    # Good: ReduceByKey
    start = time.time()
    good_result = kv_rdd.reduceByKey(lambda a, b: a + b)
    good_result.count()
    good_time = time.time() - start
    
    print(f"   GroupByKey: {bad_time:.3f}s")
    print(f"   ReduceByKey: {good_time:.3f}s")
    print(f"   ✓ Speedup: {bad_time/good_time:.2f}x faster!")
    
    # Optimization 3: Repartitioning
    print("\n🔧 Optimization 3: Repartitioning")
    original_parts = rdd_numbers.getNumPartitions()
    repartitioned = rdd_numbers.repartition(8)
    new_parts = repartitioned.getNumPartitions()
    
    print(f"   Original partitions: {original_parts}")
    print(f"   After repartition: {new_parts}")
    print(f"   ✓ Better parallelism for computation")


def performance_comparison(cached_rdd):
    """Compare cached vs uncached performance"""
    print("\n" + "="*60)
    print("STEP 4: Performance Comparison")
    print("="*60)
    
    # Create uncached version
    uncached = cached_rdd.unpersist()
    uncached = cached_rdd.map(lambda x: x)  # New lineage
    
    # Test on uncached
    print("\n⏱️  Testing UNCACHED RDD:")
    start = time.time()
    result1 = uncached.filter(lambda x: x > 1000000).count()
    time1 = time.time() - start
    print(f"   Run 1: {time1:.3f}s")
    
    start = time.time()
    result2 = uncached.filter(lambda x: x > 1000000).count()
    time2 = time.time() - start
    print(f"   Run 2: {time2:.3f}s")
    
    # Cache and test
    print("\n⏱️  Testing CACHED RDD:")
    cached_rdd.persist(StorageLevel.MEMORY_ONLY)
    
    start = time.time()
    result3 = cached_rdd.filter(lambda x: x > 1000000).count()
    time3 = time.time() - start
    print(f"   Run 1 (cold): {time3:.3f}s")
    
    start = time.time()
    result4 = cached_rdd.filter(lambda x: x > 1000000).count()
    time4 = time.time() - start
    print(f"   Run 2 (warm): {time4:.3f}s")
    
    print(f"\n   ✓ Cached is {time1/time4:.2f}x faster!")


def view_storage_info(spark):
    """Display storage information"""
    print("\n" + "="*60)
    print("STEP 5: Storage Information")
    print("="*60)
    
    sc = spark.sparkContext
    rdd_infos = sc._jsc.sc().getRDDStorageInfo()
    
    print(f"\n📊 Total Cached RDDs: {len(rdd_infos)}")
    
    if len(rdd_infos) > 0:
        print("\nCached RDD Details:")
        for i, info in enumerate(rdd_infos, 1):
            name = info.name()
            partitions = info.numPartitions()
            cached_parts = info.numCachedPartitions()
            memory_mb = info.memSize() / (1024 * 1024)
            disk_mb = info.diskSize() / (1024 * 1024)
            storage_level = info.storageLevel().description()
            
            print(f"\n{i}. {name}")
            print(f"   Partitions: {cached_parts}/{partitions} cached")
            print(f"   Memory: {memory_mb:.2f} MB")
            print(f"   Disk: {disk_mb:.2f} MB")
            print(f"   Storage: {storage_level}")


def main():
    """Main function"""
    print("\n" + "="*60)
    print("SIMPLE RDD OPTIMIZATION & STORAGE DEMO")
    print("="*60)
    
    # Create Spark
    spark = create_spark()
    
    print("\n⏳ Waiting for Spark UI to start...")
    print("   Open http://localhost:4040 in your browser")
    time.sleep(3)
    
    try:
        # Step 1: Create RDDs
        input("\n▶️  Press ENTER to create RDDs...")
        rdd_numbers, rdd_pairs = create_sample_rdds(spark)
        
        # Step 2: Apply storage levels
        input("\n▶️  Press ENTER to apply storage levels...")
        cached_rdds = demonstrate_storage_levels(rdd_numbers, rdd_pairs)
        
        # Step 3: Optimization techniques
        input("\n▶️  Press ENTER for optimization demo...")
        optimization_demo(rdd_numbers)
        
        # Step 4: Performance comparison
        input("\n▶️  Press ENTER for performance comparison...")
        performance_comparison(cached_rdds[0])
        
        # Step 5: View storage info
        input("\n▶️  Press ENTER to view storage info...")
        view_storage_info(spark)
        
        # Keep UI alive
        print("\n" + "="*60)
        print("✅ DEMO COMPLETE!")
        print("="*60)
        print("\n🌐 Spark UI: http://localhost:4040")
        print("   📊 Storage Tab: http://localhost:4040/storage/")
        print("   📋 Jobs Tab: http://localhost:4040/jobs/")
        print("   📈 Stages Tab: http://localhost:4040/stages/")
        
        input("\n▶️  Press ENTER to cleanup and exit...")
        
        # Cleanup
        for rdd in cached_rdds:
            rdd.unpersist()
        print("\n✓ All RDDs unpersisted")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        spark.stop()
        print("\n✓ Spark session stopped\n")


if __name__ == "__main__":
    main()
