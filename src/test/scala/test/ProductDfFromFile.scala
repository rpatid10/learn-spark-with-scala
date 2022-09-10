package test

import org.apache.spark.sql.SparkSession

object SparkDfFromFile extends App {

  //create spark session //entry point for spark application
  val spark = SparkSession.builder()
    .master("local[1]").appName("First Spark Application")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType};
  val dataschema = new StructType().add(StructField("id", IntegerType, true)).add(StructField("ProductName", StringType, true)).add(StructField("Tax", IntegerType, true)).add(StructField("Cost", IntegerType, true))
  val df = spark.read.format("csv")
    .option("delimiter", ",")
    .schema(dataschema)
    .load("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/InputProduct.csv")
  df.show()

  val df2 = spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .load("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/InputProductWithSchema.csv")
  println("dataframe with schema")
  df2.show()
}
