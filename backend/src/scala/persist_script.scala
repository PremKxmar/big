import org.apache.spark.storage.StorageLevel

// Create RDD with numbers 1 to 100
val data = sc.parallelize(1 to 100)

// Persist with MEMORY_ONLY storage level
val persistdata = data.persist(StorageLevel.MEMORY_ONLY)
persistdata.setName("persist_data_1_to_100")

// Perform operations
println("=" * 60)
println("Count: " + persistdata.count())
println("Sum: " + persistdata.sum())
println("=" * 60)

println("\nOpen Spark UI at: http://localhost:4040/storage/")
println("Sleeping for 60 seconds to view in Spark UI...")
println("Press Ctrl+C to exit early\n")

Thread.sleep(60000)

println("\nDone!")
