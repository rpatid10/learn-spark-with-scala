package spark.df.chapter_4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType,StructField, StructType}

import scala.language.postfixOps

object SparkDataframeFromCollection extends App{

   /*
  SparkSession was introduced in version Spark 2.0.
   It is an entry point to underlying Spark functionality in order to programmatically create Spark RDD, DataFrame, and DataSet.
  SparkSession’s object spark is the default variable available in spark-shell and it can be created programmatically using SparkSession builder pattern.
  */

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkDataframeFromCollection")
      .getOrCreate()
    //print  only "Error" messages on terminal
    spark.sparkContext.setLogLevel("ERROR")


  val dataschema = new StructType()
                       .add(StructField("id", IntegerType, true))
                       .add(StructField("ProductName", StringType, true))
                       .add(StructField("Tax", IntegerType, true))
                       .add(StructField("Cost", IntegerType, true))
  val df = spark.read.format("csv")
    .option("delimiter", ",")
    .schema(dataschema)
    .load("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/Input/InputProduct.csv")
  println("Contents Of DataFrame  : ")
  df.show()
  println("----------------------------------------------------------------------------------------")

  println("Schema of dataframe : ")
df.printSchema()
  println("----------------------------------------------------------------------------------------")

  print("count of dataframe : ")
  println(df.count())
  println("----------------------------------------------------------------------------------------")
  print("Number of partition using getNumPartitions  : ")
  println(df.rdd.getNumPartitions)
  print("Number of partition using partitions.length  : ")
  println(df.rdd.partitions.length)
  print("Number of partition using partitions.size  : ")
  println(df.rdd.partitions.size)
  println("----------------------------------------------------------------------------------------")

/*
Repartition: 

Increase or decrease partitions.
Repartition always involves a shuffle.
Repartition works by creating new partitions and doing a full shuffle to move data around.
Results in more or less equal sized partitions.
Since a full shuffle takes place, repartition is less performant than coalesce.
*/
   
  println("Increase Number Of Partition Using repartition : ")
  val df_repartition=df.repartition(3)
  print("Number of partition using getNumPartitions  : ")
  println(df_repartition.rdd.getNumPartitions)
  print("Number of partition using partitions.length  : ")
  println(df_repartition.rdd.partitions.length)
  print("Number of partition using partitions.size  : ")
  println(df_repartition.rdd.partitions.size)
  println("----------------------------------------------------------------------------------------")

  /*
  
 Coalesce:
 
Only decrease the number of partitions.
Coalesce doesn’t involve a full shuffle.
If the number of partitions is reduced from 5 to 2. Coalesce will not move data in 2 executors and move the data from the remaining 3 executors to the 
2 executors. Thereby avoiding a full shuffle.
Because of the above reason the partition size vary by a high degree.
Since full shuffle is avoided, coalesce is more performant than repartition.
Finally, When you call the repartition() function, Spark internally calls the coalesce function with shuffle parameter set to true.
*/
   
  println("Decrease Number Of Partition Using coalesce : ")
  val df_coalesce=df_repartition.coalesce(2)
  print("Number of partition using getNumPartitions  : ")
  println(df_coalesce.rdd.getNumPartitions)
  print("Number of partition using partitions.length  : ")
  println(df_coalesce.rdd.partitions.length)
  print("Number of partition using partitions.size  : ")
  println(df_coalesce.rdd.partitions.size)
  println("----------------------------------------------------------------------------------------")

}
