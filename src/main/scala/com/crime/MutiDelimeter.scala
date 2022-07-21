package com.crime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object MutiDelimeter extends App {

  val spark = SparkSession.builder().appName("Par_Data").master("local").getOrCreate()

  val mulDF = spark.read.textFile(path = "F:\\AVD\\Spark_Practice_Data\\MultiDelimeter.csv")

  mulDF.show()
  mulDF.printSchema()

  import spark.implicits._

/*
  val a = mulDF.map(i => i.mkString.split("\\~\\|"))
  a.show()
*/

  val b = mulDF.map(i => i.mkString.split("\\~\\|").mkString("\t"))
  b.show()

  val c = spark.read.option("delimiter","\t").option("header","true").csv(b)

  c.show()

}
