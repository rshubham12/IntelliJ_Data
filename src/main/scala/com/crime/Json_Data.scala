package com.crime

import org.apache.spark.sql.SparkSession

object Json_Data extends App {

  val spark = SparkSession.builder().appName("Json_Data").master("local").getOrCreate()

  val JsonDF = spark.read
    .option("header", true)
    .option("inferschema", true)
    .json(path = "F:\\AVD\\Spark_Practice_Data\\Json_Data.json")


  JsonDF.show(3)
  JsonDF.printSchema()

  val JsonMultiDF = spark.read
    .option("header", true)
    .option("inferschema", true)
    .option("multiline", true)
    .json(path = "F:\\AVD\\Spark_Practice_Data\\Json_Data_Multiline.json")


  JsonMultiDF.show(3)
  JsonMultiDF.printSchema()

  println("Shubham Rokade")



}
