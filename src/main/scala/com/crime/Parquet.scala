package com.crime

import org.apache.spark.sql.SparkSession

object Parquet extends App{

  val spark = SparkSession.builder().appName("Par_Data").master("local").getOrCreate()

  val parDF = spark.read
    .option("header", false)
    .option("inferschema", true)
    .parquet(path = "F:\\AVD\\Spark_Practice_Data\\Parquet_Data.parquet")


  parDF.show()
  parDF.printSchema()


}
