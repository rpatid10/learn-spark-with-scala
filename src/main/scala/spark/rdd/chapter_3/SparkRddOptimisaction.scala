package spark.rdd.chapter_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel._

object SparkRddOptimisaction extends App {
  /*
  SparkSession was introduced in version Spark 2.0.
   It is an entry point to underlying Spark functionality in order to programmatically create Spark RDD, DataFrame, and DataSet.
  SparkSession’s object spark is the default variable available in spark-shell and it can be created programmatically using SparkSession builder pattern.
  */

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkRddOptimisaction")
    .getOrCreate()
  //print  only "Error" messages on terminal
  spark.sparkContext.setLogLevel("ERROR")

//creating rdd with initial partition.
  val rddFromFile = spark.sparkContext.textFile("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/Input/File1.csv",3)
  rddFromFile.collect().foreach(println)

  // cache() and persist()
  rddFromFile.cache()
  rddFromFile.persist(MEMORY_ONLY)
  rddFromFile.persist(MEMORY_ONLY_2)
  rddFromFile.persist(MEMORY_AND_DISK)
  rddFromFile.persist(MEMORY_AND_DISK_2)
  rddFromFile.persist(MEMORY_ONLY_SER)
  rddFromFile.persist(MEMORY_ONLY_SER_2)
  rddFromFile.persist(MEMORY_AND_DISK_SER)
  rddFromFile.persist(MEMORY_AND_DISK_SER_2)
  rddFromFile.persist(DISK_ONLY)
  rddFromFile.persist(DISK_ONLY_2)

  // broadcast() and Accumulator
  val rddBroadCase=spark.sparkContext.broadcast(rddFromFile)

  val accum = spark.sparkContext.longAccumulator("SumAccumulator")
  spark.sparkContext.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))
  print("Accumulator Value Is :" +accum.value)

  val rddFromFileRepartition=rddFromFile.repartition(3)
  val rddFromFileCoalesce=rddFromFile.coalesce(1)


}
