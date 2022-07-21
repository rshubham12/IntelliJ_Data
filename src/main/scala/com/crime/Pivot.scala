package com.crime

import org.apache.spark.sql.SparkSession

object Pivot extends App {

    val spark = SparkSession.builder().appName("Par_Data").master("local").getOrCreate()

    val pvtDF = spark.read
      .option("delimiter","|")
      .option("header", true)
      .option("inferschema", value = true)
      .csv("F:\\AVD\\Spark_Practice_Data\\Pivot.txt")

    pvtDF.show()
    pvtDF.printSchema()

  val resultDF = pvtDF.groupBy("Roll_No").pivot("Subject").max("Marks")

  resultDF.show()

  val withtotalDF = resultDF.withColumn("Total", resultDF ("English") + resultDF ("Marathi") + resultDF ("Math") + resultDF ("Science"))

  withtotalDF.show()
}
