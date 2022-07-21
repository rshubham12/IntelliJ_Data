package com.crime
import org.apache.spark.sql.SparkSession

object Test extends App{

  val spark = SparkSession.builder().appName("crime_Analysis").master("local").getOrCreate()
  val sc = spark.sparkContext

  val crimeDF = spark.read
    .option("header", true)
    .option("inferschema", true)
    .csv(path = "F:\\AVD\\Files\\1.csv")


  crimeDF.show(3)
  crimeDF.printSchema()

  println("Shubham Rokade")


}