package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object JsonFromMultiline extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  //read multiline json file
  val multiline_df = spark.read.option("multiline", "true")
    .json("src/main/resources/multiline-zipcode.json")
  multiline_df.printSchema()
  multiline_df.show(false)

}
