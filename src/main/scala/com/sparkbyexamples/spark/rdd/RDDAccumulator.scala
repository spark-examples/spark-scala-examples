package com.sparkbyexamples.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object RDDAccumulator extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

  val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

  rdd.foreach(x => longAcc.add(x))
  println(longAcc.value)
}
