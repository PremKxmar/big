from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time


def main():
    # Create Spark Session
    print("\n" + "="*60)
    print("Creating Spark Session")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("Persistence Example") \
        .master("local[*]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"\n✓ Spark Session Created!")
    print(f"  Version: {spark.version}")
    print(f"  UI: http://localhost:4040")
    print("="*60)
    
    # Get Spark Context
    sc = spark.sparkContext
    
    # Create RDD with numbers 1 to 100
    print("\n📊 Creating RDD with numbers 1 to 100...")
    data = sc.parallelize(range(1, 101))
    
    # Persist with MEMORY_ONLY storage level
    print("💾 Persisting RDD with MEMORY_ONLY storage level...")
    persistdata = data.persist(StorageLevel.MEMORY_ONLY)
    persistdata.setName("persist_data_1_to_100")
    
    # Perform operations
    print("\n" + "="*60)
    print("Performing Operations on Cached RDD")
    print("="*60)
    
    count_result = persistdata.count()
    print(f"\n✓ Count: {count_result}")
    
    sum_result = persistdata.sum()
    print(f"✓ Sum: {sum_result}")
    
    # Keep alive for UI viewing
    print("\n" + "="*60)
    print("🌐 VIEW IN SPARK UI")
    print("="*60)
    print(f"\n  Main UI: http://localhost:4040")
    print(f"  Storage Tab: http://localhost:4040/storage/")
    print(f"\n  You should see the cached RDD: 'persist_data_1_to_100'")
    print(f"  Storage Level: MEMORY_ONLY")
    print(f"  Size in Memory: ~800 bytes")
    print("\n⏳ Sleeping for 60 seconds to keep UI alive...")
    print("   Press Ctrl+C to exit early\n")
    
    try:
        time.sleep(60)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    
    print("\n✓ Stopping Spark session...")
    spark.stop()
    print("✓ Done!\n")


if __name__ == "__main__":
    main()
