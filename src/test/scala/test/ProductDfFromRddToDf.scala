package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

object ProductDfFromRddToDf extends App {

  //create spark session //entry point for spark application
  val spark = SparkSession.builder()
    .master("local[1]").appName("First Spark Application")
    .getOrCreate()

  val data = Seq(("Mobile", "10000"), ("Mobile", "20000"), ("EarPhone", "2000"), ("Shirt", "1000"))

  //creating Rdd
  val rdd = spark.sparkContext.parallelize(data)
  //taking Rdd Count
  rdd.count()

  import spark.implicits._

  //convert rdd to dataframe using toDF method
  val rdddf = rdd.toDF("Product", "Price")

  //display the content of dataframe on console
  rdddf.show()
  //setting logger level as error
  spark.sparkContext.setLogLevel("ERROR")
  //print schema details
  rdddf.printSchema()
  //get number of partition of dataframe
  println("Number Of Partitions :" + rdddf.rdd.getNumPartitions)
  //create temp view/table
  rdddf.createOrReplaceTempView("tmp")
  // applying aggregation function //wide transformation
  val rdddf2 = spark.sql("select sum(Price),Product from tmp group by Product")
  //get number of partition of dataframe after aggregation function //wide transformation
  println("Number Of Partitions :" + rdddf2.rdd.getNumPartitions)
  // Increase number of partition using repartition method
  val dff3 = rdddf2.repartition(300)
  //get number of partition of dataframe
  println("Number Of Partitions :" + dff3.rdd.getNumPartitions)
  // Increase number of partition using coalesce method
  val dff4 = dff3.coalesce(5)
  //get number of partition of dataframe
  println("Number Of Partitions After Colease " + dff4.rdd.getNumPartitions)
  //adding new column in dataframe using spark-sql
  val dff5 = spark.sql("select 'Rahul' as FirstName,'Patidar' as LastName, sum(Price) as Total_Price,Product from tmp group by Product")
  dff5.show()
  dff5.printSchema()
  //adding new column using spark-dataframe API.

  val dff6 = dff5.withColumn("Company", lit("Jio"))
  dff6.show()
  dff6.printSchema()

  //drop column using spark-dataframe API.
  val dff7 = dff6.drop("LastName")
  dff7.show()
  dff7.printSchema()

  //Rename a existing column using spark-dataframe API.
  val dff8 = dff7.withColumnRenamed("FirstName", "Name")
  dff8.show()
  dff8.printSchema()

  //Derive a new column from existing
  val dff9 = dff8.withColumn("GST", col("Total_Price") * 18 / 100)
  dff9.show()
  dff9.printSchema()

  //creting new column using case statement
  val dff10 = dff9.withColumn("Status", when(col("Total_Price") >= 10000, lit("Iphone")).otherwise(lit("Other Stuff")))
  //decrease partition using coalesce, If we do not decrease partition here ,
  // then spark will create 200 files as after shuffling
  // spark will craete 200 partition (defalut value for. spark.sql.shuffle.partitions ).
  // based on your data size you may need to reduce or increase the number of partitions of RDD/DataFrame before writing.
  val dff11 = dff10.coalesce(1)

  // writing dataframe in csv format.
  dff11.write.csv("/Users/rahul1.patidar/Desktop/SparkKt/SparkLearning/out/Test/")

}
