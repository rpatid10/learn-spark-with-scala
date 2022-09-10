name := "SparkLearning"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0",
  "biz.paluch.logging" % "logstash-gelf" % "1.12.0",
  "org.apache.cxf" % "cxf-core" % "3.1.0"
)

