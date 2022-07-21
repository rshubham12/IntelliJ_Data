package com.crime

import org.apache.spark.sql.SparkSession

object Emp_details extends App{

  val spark = SparkSession.builder().appName("Par_Data").master("local").getOrCreate()

  val Emp_rdd = spark.read.textFile("F:\\AVD\\Spark_Practice_Data\\Emp_details.txt")

  Emp_rdd.show()
  Emp_rdd.printSchema()
  Emp_rdd.collect().foreach(println)

  import spark.implicits._

  /* Using Case Class */
  case class trans_rdd(id:Int,name:String,city:String)

  val trans_rdd1 = Emp_rdd.map(line => {
      val col = line.split(",")

    trans_rdd(col(0).toInt,
              col(1),
              col(2))
  })

  trans_rdd1.show()

  /* Using Collect */
  val trans_rdd2 = Emp_rdd.map(line => {
    val col = line.split(",")
    (col(0),col(1), col(2))})

  trans_rdd2.collect().foreach(println)

  /* Using Take */
  val trans_rdd3 = Emp_rdd.map(line => {
    val col = line.split(",")
    (col(0),col(1), col(2))})

  trans_rdd3.take(2).foreach(println)

}
