package com.df
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, spark_partition_id}

object Count_Partitions {

    // How to find the count of each partition in spark Dataframe.

    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("Partition_Data_Count").master("local").getOrCreate()

      // Disable unwanted Information

      val sc = spark.sparkContext
      sc.setLogLevel("OFF")

      /**   Creating Spark DF from CSV  */

      val total_df = spark.read
        .option("header", true)
        .option("inferschema", true)
        .csv("F:\\AVD\\Spark_Practice_Data\\Input\\Count_Partition.txt")

      total_df.show()

      val part_df = total_df.repartition(4)

      part_df.rdd.getNumPartitions

      //Using Spark Partition Id Function...
      val counted_df1 = part_df.groupBy(spark_partition_id).count().orderBy(asc("count"))
      val counted_df2 = part_df.withColumn("Partitioned_ID",spark_partition_id).groupBy("Partitioned_ID").count()
      val counted_df3 = part_df.groupBy(spark_partition_id).count()

      counted_df1.show()
      counted_df2.show()
      counted_df3.show()

    }



}
