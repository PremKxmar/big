from pyspark.sql import SparkSession


def main():
    # Create Spark Session
    print("\n" + "="*60)
    print("Employee DataFrame Demo")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("Employee Data Demo") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"\n✓ Spark Session Created (Version: {spark.version})")
    
    # Read employee CSV file
    print("\n📂 Reading employee_data.csv...")
    
    csv_path = "c:/sem6-real/vscode2/SmartCityTrafficSystem/data/raw/employee_data.csv"
    
    df = spark.read.csv(
        csv_path,
        header=True,           # First row contains column names
        inferSchema=True       # Automatically detect data types
    )
    
    print("✓ DataFrame created successfully!")
    
    # Display Schema
    print("\n" + "="*60)
    print("DataFrame Schema")
    print("="*60)
    df.printSchema()
    
    # Show all employee data
    print("\n" + "="*60)
    print("Employee Data")
    print("="*60)
    df.show()
    
    # Display statistics
    print("\n" + "="*60)
    print("DataFrame Information")
    print("="*60)
    print(f"Total Employees: {df.count()}")
    print(f"Columns: {df.columns}")
    
    # Summary statistics
    print("\n" + "="*60)
    print("Summary Statistics")
    print("="*60)
    df.describe().show()
    
    # Filter employees with salary > 60000
    print("\n" + "="*60)
    print("Employees with Salary > 60000")
    print("="*60)
    df.filter(df.salary > 60000).show()
    
    # Average salary by age group
    print("\n" + "="*60)
    print("Average Salary")
    print("="*60)
    from pyspark.sql.functions import avg
    df.select(avg("salary")).show()
    
    # Sort by salary descending
    print("\n" + "="*60)
    print("Employees Sorted by Salary (Highest to Lowest)")
    print("="*60)
    df.orderBy(df.salary.desc()).show()
    
    print("\n✓ Done!\n")
    spark.stop()


if __name__ == "__main__":
    main()
