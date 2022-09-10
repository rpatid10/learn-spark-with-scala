package spark.rdd.chapter_2

import org.apache.spark.sql.SparkSession

object SparkPairRddOperations extends App {
  /*
  SparkSession was introduced in version Spark 2.0.
   It is an entry point to underlying Spark functionality in order to programmatically create Spark RDD, DataFrame, and DataSet.
  SparkSession’s object spark is the default variable available in spark-shell and it can be created programmatically using SparkSession builder pattern.
  */

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkPairRddOperations")
    .getOrCreate()
  //print  only "Error" messages on terminal
  spark.sparkContext.setLogLevel("ERROR")

  // create pair RDD

  val rdd = spark.sparkContext.parallelize(
    List("Spark Is a Computation Engine", "Hive Is a DataWareHouse", "Hadoop Is a Framework.")
  )

  /*
 Spark flatMap() transformation flattens the DataFrame/Dataset/Rdd after applying the function on every element and returns a new transformed DataFrame/Dataset/RDD.
 The returned DataFrame/Dataset/RDD will return more rows than the current DataFrame/Dataset/RDD. It is also referred to as a one-to-many transformation function. So here If we apply
 flatMap() on out List basically It will apply transformation on each elements of a row(Spark Is a Computation Engine) here it will split row based on
 ' ' and generate 4 rows from a single row.( one-to-many transformation)
  */
  //split each element of a line
  val wordsRdd = rdd.flatMap(_.split(" "))
  println("output of flatmap is : ")
  wordsRdd.foreach(println)
  println("---------------------------------------------------------------")

  /*
  Spark map() transformation applies a function to each row in a DataFrame/Dataset/RDD and returns the new transformed DataFrame/Dataset/RDD.
  (one-to-one transformation)
   */
  val pairRDD = wordsRdd.map(f => (f, 1))
  println("output of map is : ")
  pairRDD.foreach(println)
  println("---------------------------------------------------------------")

  //reduceByKey – Transformation returns an RDD after adding value for each key.

  println("Reduce by Key ==>")
  val wordCount = pairRDD.reduceByKey((a, b) => a + b)
  wordCount.foreach(println)
  println("---------------------------------------------------------------")

  // distinct – Returns distinct keys.
  println("distinct keys From Rdd are : ")
  pairRDD.distinct().foreach(println)
  println("---------------------------------------------------------------")

  //sortByKey – Transformation returns an RDD after sorting by key

  println("Sort by Key ==>")
  val sortRDD = pairRDD.sortByKey()
  sortRDD.foreach(println)

  println("---------------------------------------------------------------")

  //aggregateByKey – Transformation same as reduceByKey

  def param1 = (accu: Int, v: Int) => accu + v

  def param2 = (accu1: Int, accu2: Int) => accu1 + accu2

  println("Aggregate by Key ==> wordcount")
  val wordCount2 = pairRDD.aggregateByKey(0)(param1, param2)
  wordCount2.foreach(println)
  println("---------------------------------------------------------------")

  //keys – Return RDD[K] with all keys in an dataset
  //keys
  println("Keys ==>")
  wordCount2.keys.foreach(println)
  println("---------------------------------------------------------------")

  //values
  println("values ==>")
  wordCount2.values.foreach(println)
  println("---------------------------------------------------------------")

  println("Count :" + wordCount2.count())
  println("---------------------------------------------------------------")

/* collectAsMap will return the results for paired RDD as Map collection.
  And since it is returning Map collection you will only get pairs with unique keys and pairs with duplicate keys will be removed.
*/
  println("collectAsMap ==>")
  pairRDD.collectAsMap().foreach(println)
println("---------------------------------------------------------------")

/*
  Spark RDD reduce() aggregate action function is used to calculate min, max, and total of elements in a dataset/dataset/rdd.
  RDD reduce() function takes function type as an argument and returns the RDD with the same type as input.
  It reduces the elements of the input RDD using the binary operator specified.
*/
  val listRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))
  println("output min : " + listRdd.reduce(_ min _))
  println("output max : " + listRdd.reduce(_ max _))
  println("output sum  : " + listRdd.reduce(_ + _))

}
