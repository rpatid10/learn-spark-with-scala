package spark.rdd.chapter_1

/*
 Different Ways to Create Spark Rdd:
 --------------------------------------------------
1. Spark create RDD from Seq or List  (using Parallelize)
2. Creating an RDD from a text file
3. Creating from another RDD
4. Creating from existing DataFrames and DataSet (Will Cover this method with Dataframes/DataSet.)
*/



//Spark Rdd from Seq or List  (using Parallelize)
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps

object SparkRddFromParallelize extends App {

/*
SparkSession was introduced in version Spark 2.0.
 It is an entry point to underlying Spark functionality in order to programmatically create Spark RDD, DataFrame, and DataSet.
SparkSession’s object spark is the default variable available in spark-shell and it can be created programmatically using SparkSession builder pattern.
*/

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkRddFromParallelize")
    .getOrCreate()
//print  only "Error" messages on terminal
  spark.sparkContext.setLogLevel("ERROR")

  val dataList=List("Spark Is a Computation Engine", "Hive Is a DataWareHouse", "Hadoop Is a Framework")
  val rddFromList = spark.sparkContext.parallelize(dataList)


  //Create RDD from parallelize
  val dataSeq = Seq(("Rahul", 20000), ("Avinash", 100000), ("Ashish", 30000),("Ashish", 40000))
  val rddFromSeq = spark.sparkContext.parallelize(dataSeq)


  // view  full the content of a RDD
  println(" full content of a RDD are : ")
  rddFromSeq.collect().foreach(println)
  println("--------------------------------------------------------")


  // Use take() to take just a few to print out

  println("View Only 1 row : ")
  rddFromSeq.take(1).foreach(println)
  println("--------------------------------------------------------")
  // Return the number of elements in the RDD.
  println("number of elements in the RDD is : ")
  println(rddFromSeq.count())
  println("--------------------------------------------------------")
  // Returns the number of partitions of this RDD.
  println("number of partitions of this RDD is : ")
  println(rddFromSeq.getNumPartitions)
  println("--------------------------------------------------------")
  //Return the first element in this RDD.
  println("first element in this RDD is : ")
  println(rddFromSeq.first())
  println("--------------------------------------------------------")

  // Return Result as per filter condition.Here , We are Creating New RDD from Existing  RDD.

  val rdd4 = rddFromSeq.filter(a=> a._1.startsWith("R"))
  println(" elements which starts with 'R' is : " )
  rdd4.collect().foreach(println)
  println("--------------------------------------------------------")
  // reduceByKey() merges the values for each key.
  val rdd5 = rddFromSeq.reduceByKey(_ + _)
  println(" Merge Values for each key is : " )
  rdd5.collect().foreach(print)
  println("--------------------------------------------------------")
  //sortByKey() transformation is used to sort RDD elements on key

  val rdd6 = rddFromSeq.map(a => (a._1, a._2)).sortByKey()
  //Print rdd6 result to console
  println(" sorted elements based on key are : " )
  rdd6.foreach(println)
  println("--------------------------------------------------------")
  // print only the first column
  val rdd7 = rddFromSeq.map(a => (a._1))
  //print rdd7 result to console
  println("elements of first column using map are : ")
  rdd7.foreach(println)
  println("--------------------------------------------------------")
  //convert value into (key,value) pair using map
  val rdd8 = rddFromSeq.map(a=>(a._1,1))
  println("(key,value) pair using map are : ")
  rdd8.foreach(println)
  println("--------------------------------------------------------")
// Saving Rdd As TextFile.
  println("Saving file at : '//Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/OutPut'"
             +rddFromSeq.saveAsTextFile("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/OutPut/"))
  println("--------------------------------------------------------")
  println("Initial partition of rdd :"+rddFromSeq.getNumPartitions)
  println("--------------------------------------------------------")
  /*
  increasing partition from intial partition '1' to '2'.
  This will now split data into 2 output files.
   */
  val rdd10=rddFromSeq.repartition(2)
  println("New partition of rdd :"+rdd10.getNumPartitions)
  println("--------------------------------------------------------")


  println("Saving file at : '/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/OutPut/repartition/'"
    + rdd10.saveAsTextFile("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/OutPut/repartition/"))
  println("--------------------------------------------------------")
/*
   we can also change the partition using coalesce,
   when we use coalesce it will only decrese the partition.
   we can not increase partition using coalesce.
   */
  val rdd11 = rdd10.coalesce(1)
  println("New partition of rdd :"+rdd11.getNumPartitions)
  import spark.implicits._
  //Convert rdd to Dataframe:
  val dfFromRDD1 = rdd11.toDF()
  // Display Data from DataFrame
  println("Contents of DataFrame: "+dfFromRDD1.show())
  val rddFromDf=dfFromRDD1.rdd
  // Display Data from rdd
  println("Contents of Rdd : ")
  rddFromDf.collect().foreach(println)
}
