package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when

object ProductAnalysis extends App {

  //create spark session //entry point for spark application
  val spark = SparkSession.builder()
    .master("local[1]").appName("Product Analysis")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val Products = Seq(
    (1, "Mobile", 20, 8000),
    (2, "Cloths", 20, 3000),
    (3, "Shoe", 40, 2000),
    (4, "AC", 800, 15000),
    (5, "Wallet", 10, 540)
  ).toDF("Product_ID", "Product_Name", "Tax", "Cost")
  val Customers = Seq(("Kapil", "Mumbai", 2, 2),
    ("Rahul", "Bhopal", 3, 3),
    ("Avinash", "Bangalore", 4, 2),
    ("Ashish", "Chennai", 5, 6),
    ("Ankit", "indore", 1, 7),
    ("Kapil", "Mumbai", 4, 3),
    ("Rahul", "Bhopal", 5, 1),
    ("Avinash", "Bangalore", 2, 1),
    ("Ashish", "Chennai", 3, 6),
    ("Ankit", "indore", 3, 1))
    .toDF("Customer_Name", "City", "Product", "Qty")
  Products.show()
  Customers.show()
  val custInChennai = Customers
    .join(Products, $"Product" === $"Product_ID", "outer")
    .select("Customer_Name", "City", "Product_ID", "Qty", "Product_Name", "Tax", "Cost")
    .filter($"City" === "Chennai")
    .withColumn("TotalPrice", ($"Tax".cast("Int") * $"Qty".cast("Int") * $"Cost".cast("Int")))
    .show()
  val customerClassification = Customers
    .join(Products, $"Product" === $"Product_ID", "outer")
    .select("Customer_Name", "City", "Product_ID", "Qty", "Product_Name", "Tax", "Cost")
    .withColumn("TotalPrice", ($"Tax".cast("Int") * $"Qty".cast("Int") * $"Cost".cast("Int")))
    .groupBy("Customer_Name")
    .agg(Map("TotalPrice" -> "sum"))
    .withColumnRenamed("sum(TotalPrice)", "TotalPrice")
  customerClassification.show()

  val customerClassified = customerClassification
    .withColumn("CustomerExpenseType", (when($"TotalPrice".cast("Int") <= 245400, "Low")
      .when($"TotalPrice".cast("Int") >= 1200000 && $"TotalPrice".cast("Int") <= 1500000, "Middle")
      .when($"TotalPrice".cast("Int") >= 1500001 && $"TotalPrice".cast("Int") <= 2000000, "Upper Middle")
      .otherwise("High")))

  customerClassified.show()

  val toptwocustomer = Customers
    .join(Products, $"Product" === $"Product_ID", "outer")
    .select("Customer_Name", "City", "Product_ID", "Qty", "Product_Name", "Tax", "Cost")
    .withColumn("TotalPrice", ($"Tax".cast("Int") * $"Qty".cast("Int") * $"Cost".cast("Int")))
    .groupBy("Customer_Name")
    .agg(Map("TotalPrice" -> "sum"))
    .withColumnRenamed("sum(TotalPrice)", "TotalPrice")
    .sort($"TotalPrice".desc)


}
