"""
Convert Parquet Files to Readable CSV Format
=============================================

This script converts Parquet files to CSV format for easy viewing.
Creates sample CSVs and summary files in data/readable/ folder.

Usage:
    python scripts/export_readable_data.py
"""

import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set up Hadoop environment for Windows
if os.name == 'nt':
    hadoop_home = r"C:\hadoop"
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['hadoop.home.dir'] = hadoop_home
    if hadoop_home not in os.environ.get('PATH', ''):
        os.environ['PATH'] = os.environ.get('PATH', '') + f";{hadoop_home}\\bin"

from pyspark.sql import SparkSession

# Configuration
PROCESSED_DIR = PROJECT_ROOT / "backend" / "data" / "processed"
READABLE_DIR = PROJECT_ROOT / "data" / "readable"

def create_spark_session():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("ParquetToCSV-Export") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def export_parquet_to_csv(spark, parquet_path, output_name, sample_rows=1000):
    """Export a Parquet file/directory to CSV with sample rows."""
    print(f"\nExporting: {parquet_path.name}")
    
    try:
        df = spark.read.parquet(str(parquet_path))
        total_rows = df.count()
        num_cols = len(df.columns)
        
        print(f"  Total rows: {total_rows:,}")
        print(f"  Columns: {num_cols}")
        
        # Create output directory
        output_dir = READABLE_DIR / output_name
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export schema info
        schema_file = output_dir / "schema.txt"
        with open(schema_file, "w") as f:
            f.write(f"Parquet File: {parquet_path.name}\n")
            f.write(f"Total Rows: {total_rows:,}\n")
            f.write(f"Total Columns: {num_cols}\n")
            f.write("=" * 60 + "\n\n")
            f.write("SCHEMA:\n")
            f.write("-" * 40 + "\n")
            for field in df.schema.fields:
                f.write(f"  {field.name}: {field.dataType.simpleString()}\n")
        print(f"  ✓ Schema saved: {schema_file.name}")
        
        # Export sample data as CSV
        sample_df = df.limit(sample_rows).toPandas()
        csv_file = output_dir / f"sample_{sample_rows}_rows.csv"
        sample_df.to_csv(csv_file, index=False)
        print(f"  ✓ Sample CSV saved: {csv_file.name}")
        
        # Export full data if small enough
        if total_rows <= 100000:
            full_csv = output_dir / "full_data.csv"
            df.toPandas().to_csv(full_csv, index=False)
            print(f"  ✓ Full data CSV saved: {full_csv.name}")
        else:
            print(f"  (Skipping full export - too large: {total_rows:,} rows)")
        
        # Export statistics summary
        stats_file = output_dir / "statistics.txt"
        with open(stats_file, "w") as f:
            f.write(f"STATISTICS for {parquet_path.name}\n")
            f.write("=" * 60 + "\n\n")
            
            # Get numeric columns
            numeric_cols = [c for c, t in df.dtypes if t in ('double', 'float', 'int', 'bigint', 'long')]
            
            if numeric_cols:
                stats_df = df.select(numeric_cols).describe().toPandas()
                f.write(stats_df.to_string())
        print(f"  ✓ Statistics saved: {stats_file.name}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def main():
    print("=" * 60)
    print("EXPORTING PARQUET FILES TO READABLE CSV FORMAT")
    print("=" * 60)
    print(f"Source: {PROCESSED_DIR}")
    print(f"Output: {READABLE_DIR}")
    
    # Create output directory
    READABLE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Find all Parquet files/directories
    parquet_items = []
    
    # Check for Parquet directories
    for item in PROCESSED_DIR.iterdir():
        if item.is_dir() and "parquet" in item.name.lower():
            parquet_items.append(item)
        elif item.is_file() and item.suffix == ".parquet":
            parquet_items.append(item)
    
    if not parquet_items:
        print("\nNo Parquet files found!")
        return
    
    print(f"\nFound {len(parquet_items)} Parquet items:")
    for p in parquet_items:
        print(f"  - {p.name}")
    
    # Export each
    success_count = 0
    for parquet_path in parquet_items:
        output_name = parquet_path.name.replace(".parquet", "")
        if export_parquet_to_csv(spark, parquet_path, output_name):
            success_count += 1
    
    spark.stop()
    
    print("\n" + "=" * 60)
    print("EXPORT COMPLETE")
    print("=" * 60)
    print(f"Exported: {success_count}/{len(parquet_items)} files")
    print(f"Output location: {READABLE_DIR}")
    print("\nFiles created for each dataset:")
    print("  - schema.txt (column names and types)")
    print("  - sample_1000_rows.csv (first 1000 rows)")
    print("  - statistics.txt (min, max, mean, etc.)")
    print("  - full_data.csv (if <= 100,000 rows)")


if __name__ == "__main__":
    main()
