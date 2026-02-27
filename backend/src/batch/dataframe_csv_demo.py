"""
Simple DataFrame from CSV Demo
===============================

Demonstrates:
- Creating a Spark DataFrame from a CSV file
- Displaying DataFrame schema
- Showing data
- Basic operations on DataFrame
"""

from pyspark.sql import SparkSession


def main():
    # Create Spark Session
    print("\n" + "="*60)
    print("Creating Spark Session")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("DataFrame CSV Demo") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"\n✓ Spark Session Created!")
    print(f"  Version: {spark.version}")
    print("="*60)
    
    # Read CSV file into DataFrame
    print("\n📂 Reading CSV file...")
    
    # Using sample traffic data from your project
    csv_path = "c:/sem6-real/vscode2/SmartCityTrafficSystem/data/raw/yellow_tripdata_2015-01.csv"
    
    df = spark.read.csv(
        csv_path,
        header=True,           # First row is header
        inferSchema=True       # Automatically infer data types
    )
    
    print(f"✓ DataFrame created from CSV!")
    
    # Display Schema
    print("\n" + "="*60)
    print("DataFrame Schema")
    print("="*60)
    df.printSchema()
    
    # Show sample data
    print("\n" + "="*60)
    print("Sample Data (First 10 rows)")
    print("="*60)
    df.show(10)
    
    # Basic operations
    print("\n" + "="*60)
    print("DataFrame Statistics")
    print("="*60)
    
    total_rows = df.count()
    total_cols = len(df.columns)
    
    print(f"\n✓ Total Rows: {total_rows:,}")
    print(f"✓ Total Columns: {total_cols}")
    print(f"✓ Column Names: {df.columns}")
    
    # Select specific columns
    print("\n" + "="*60)
    print("Selected Columns (pickup_datetime, passenger_count)")
    print("="*60)
    df.select("pickup_datetime", "passenger_count").show(5)
    
    # Filter operation
    print("\n" + "="*60)
    print("Filtered Data (passenger_count > 3)")
    print("="*60)
    df.filter(df.passenger_count > 3).show(5)
    
    # Group by and aggregate
    print("\n" + "="*60)
    print("Group By passenger_count (Count)")
    print("="*60)
    df.groupBy("passenger_count").count().show()
    
    # Summary statistics
    print("\n" + "="*60)
    print("Summary Statistics")
    print("="*60)
    df.describe().show()
    
    print("\n✓ Done!")
    spark.stop()


if __name__ == "__main__":
    main()
