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
  val rddFromFile = spark.sparkContext.textFile("/Users/rahul/Desktop/SparkLearning/out/Test/Input/File1.csv",3)
  rddFromFile.collect().foreach(println)

  /*
Using cache() and persist() methods, Spark provides an optimization mechanism to store the intermediate computation of an RDD, DataFrame, and Dataset
so they can be reused in subsequent actions(reusing the RDD, Dataframe, and Dataset computation result’s).
Both caching and persisting are used to save the Spark RDD, Dataframe, and Dataset’s. But, the difference is, RDD cache() method default saves it to 
memory (MEMORY_ONLY) whereas persist() method is used to store it to the user-defined storage level.  
*/

  rddFromFile.cache()
  rddFromFile.persist(MEMORY_ONLY)
  
 /* 
  rddFromFile.persist(MEMORY_AND_DISK)
  rddFromFile.persist(MEMORY_ONLY_SER)
  rddFromFile.persist(MEMORY_AND_DISK_SER)
  rddFromFile.persist(DISK_ONLY)
  */

/*
 Accumulators and broadcast variables both are spark shared variables.
 Accumulators are special kind of variable which we basically use to update some data point across executors. 
 Broadcast variables are read only variable which will be cached in all the executors in stead of shipping every time with the tasks. 
 Basically, broadcast variables are used as lookup without any shuffle as each executor will keep a local copy of it, so no network I/O overhead 
 involves here.

*/
  
  val rddBroadCase=spark.sparkContext.broadcast(rddFromFile)

  val accum = spark.sparkContext.longAccumulator("SumAccumulator")
  spark.sparkContext.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))
  print("Accumulator Value Is :" +accum.value)

  val rddFromFileRepartition=rddFromFile.repartition(3)
  val rddFromFileCoalesce=rddFromFile.coalesce(1)


}
