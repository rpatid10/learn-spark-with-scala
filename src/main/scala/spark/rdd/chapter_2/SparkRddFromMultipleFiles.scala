package spark.rdd.chapter_2

import org.apache.spark.sql.SparkSession

object SparkRddFromMultipleFiles extends App{
  /*
  SparkSession was introduced in version Spark 2.0.
   It is an entry point to underlying Spark functionality in order to programmatically create Spark RDD, DataFrame, and DataSet.
  SparkSession’s object spark is the default variable available in spark-shell and it can be created programmatically using SparkSession builder pattern.
  */

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkRddFromMultipleFiles")
    .getOrCreate()
  //print  only "Error" messages on terminal
  spark.sparkContext.setLogLevel("ERROR")


  //Create RDD from external Data source (.csv file)

  val rddFromFile = spark.sparkContext.textFile("/Users/rahul/Desktop/SparkLearning/out/Test/Input/File1.csv")
  rddFromFile.collect().foreach(println)
    .appName("SparkRddFromMultipleFiles")
    .getOrCreate()
  //print  only "Error" messages on terminal
  spark.sparkContext.setLogLevel("ERROR")


  //Create RDD from external Data source (.csv file)

  val rddFromFile = spark.sparkContext.textFile("/Users/rahul/Desktop/SparkLearning/out/Test/Input/File1.csv")
  rddFromFile.collect().foreach(println)

// Read multiple files and create Rdd.
  println("spark read csv files from a directory into RDD")
val rddFrommultipleFile = spark.sparkContext
  .textFile("/Users/rahul/Desktop/SparkLearning/out/Test/Input/File1.csv,/Users/rahul/Desktop/SparkLearning/out/Test/Input/File2.csv")
  rddFrommultipleFile.collect().foreach(println)

}


// Read multiple files and create Rdd.
  println("spark read csv files from a directory into RDD")
val rddFrommultipleFile = spark.sparkContext
  .textFile("/Users/rahul/Desktop/SparkLearning/out/Test/Input/File1.csv,/Users/rahul/Desktop/SparkLearning/out/Test/Input/File2.csv")
  rddFrommultipleFile.collect().foreach(println)

}
