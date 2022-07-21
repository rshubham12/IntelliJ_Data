/**
package com.crime

import org.apache.spark.sql.SparkSession

object WordCount extends App {
  def main(args: Array[String]): Unit = super.main(args){

  val spark = SparkSession.builder().appName("Par_Data").master("local").getOrCreate()

  val wcDF = spark.read
    .textFile(path = "F:\\AVD\\Spark_Practice_Data\\WordCount.txt")

  wcDF.show(truncate = true)
  wcDF.show(truncate = false)


  val r2 = wcDF.map(x=>x.split(" "))
  r2.show(truncate = false)

  val r3 = r2.flatMap(x=>x)
  r3.show(truncate = false)

  val r4 = r3.map(x=>(x,1))
  r4.show(truncate = false)



    val r5 = r4.reduceByKey((x,y) => (x+y))

  r5.collect.foreach(println)

    r4.saveAsTextFile("hdfs://...")

}
}
*/