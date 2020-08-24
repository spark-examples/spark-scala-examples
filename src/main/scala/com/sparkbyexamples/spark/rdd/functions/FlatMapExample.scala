package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object FlatMapExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")
  val rdd=spark.sparkContext.parallelize(data)
  rdd.foreach(println)

  val rdd1 = rdd.flatMap(f=>f.split(" "))
  rdd1.foreach(println)

}
