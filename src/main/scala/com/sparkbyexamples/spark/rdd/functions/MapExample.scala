package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object MapExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq("Project",
  "Gutenberg’s",
  "Alice’s",
  "Adventures",
  "in",
  "Wonderland",
  "Project",
  "Gutenberg’s",
  "Adventures",
  "in",
  "Wonderland",
  "Project",
  "Gutenberg’s")

  val rdd=spark.sparkContext.parallelize(data)

  val rdd2=rdd.map(f=> (f,1))
  rdd2.foreach(println)

}
