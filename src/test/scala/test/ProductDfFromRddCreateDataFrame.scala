package test

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ProductDfFromRddCreateDataFrame extends App {

  //create spark session //entry point for spark application
  val spark = SparkSession.builder()
    .master("local[1]").appName("First Spark Application ProductDfFromRddCreateDataFrame")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val schema = StructType(Array(
    StructField("Product_ID", IntegerType, true),
    StructField("Product_Name", StringType, true),
    StructField("Tax", IntegerType, true),
    StructField("Cost", IntegerType, true))
  )


  val Products = Seq(
    (1, "shirt", 2, 800),
    (2, "Jeans", 2, 300),
    (3, "Watch", 4, 200),
    (4, "Toys", 8, 150),
    (5, "cooldrinks", 10, 540)
  )
  val rdd = spark.sparkContext.parallelize(Products)

  val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2, attributes._3, attributes._4))
  val dfFromRDD3 = spark.createDataFrame(rowRDD, schema)
  dfFromRDD3.show()

  dfFromRDD3.write.csv("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/ProductDfFromRddCreateDataFrame/")

}
