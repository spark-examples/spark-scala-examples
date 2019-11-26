package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object RDDFromDataUsingParallelize {

  def main(args: Array[String]): Unit = {
      val spark:SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName("SparkByExample")
        .getOrCreate()
      val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
      val rddCollect:Array[Int] = rdd.collect()
      println("Number of Partitions: "+rdd.getNumPartitions)
      println("Action: First element: "+rdd.first())
      println("Action: RDD converted to Array[Int] : ")
      rddCollect.foreach(println)

  }
}
