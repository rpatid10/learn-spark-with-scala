package spark.rdd.chapter_1

import org.apache.spark.sql.SparkSession

import scala.language.postfixOps


object SparkRddFromFile extends App {
  /*
SparkSession was introduced in version Spark 2.0.
 It is an entry point to underlying Spark functionality in order to programmatically create Spark RDD, DataFrame, and DataSet.
SparkSession’s object spark is the default variable available in spark-shell and it can be created programmatically using SparkSession builder pattern.
*/

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkRddFromFile")
    .getOrCreate()
  //print  only "Error" messages on terminal
  spark.sparkContext.setLogLevel("ERROR")


  //Create RDD from external Data source
  val rdd2 = spark.sparkContext.textFile("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/Input/File1.txt")
  println("Number Of Partitions In Rdd are : "+rdd2.getNumPartitions)
  println("Contents of rdd are : ")
  rdd2.collect().foreach(println)

/* Using sparkContext.wholeTextFiles()
   Reads entire file into a RDD as single record.
   wholeTextFiles() function returns a PairRDD with the key being the file path and value being file content.*/

  val rdd3 = spark.sparkContext.wholeTextFiles("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/Input/File1.txt")
  println()
  println("Contents of rdd3 are : ")
  rdd3.collect().foreach(println)


}
