/**
package com.crime

import org.apache.spark.sql.SparkSession

object WC extends App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Par_Data").master("local").getOrCreate()


    val wcDF = spark.read.textFile(path = "F:\\AVD\\Spark_Practice_Data\\WordCount.txt")

    val counts = wcDF. flatMap (line => line.split (" ")).map (word => (word, 1)).reduceByKey (_+_)

    counts.saveAsTextFile("F:\\AVD\\Spark_Practice_Data\\Output")

  }

}
*/