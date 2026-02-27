package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object persist {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("Persistance Example")
            .master("local[*]")
            .getOrCreate()

        val sc = spark.sparkContext
        val data = sc.parallelize(1 to 100)
        val persistdata = data.persist(StorageLevel.MEMORY_ONLY)

        println("Count: " + persistdata.count())
        println("Sum: " + persistdata.sum())

        println("Sleeping for 60 seconds to view in Spark UI at http://localhost:4040")
        println("Open http://localhost:4040/storage/ to see cached RDD")
        Thread.sleep(60000)
        
        spark.stop()
    }
}
