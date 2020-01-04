package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object HandleNullExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val filePath="src/main/resources/small_zipcode.csv"

  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(filePath)
  df.printSchema()
  df.show(false)

  df.na.fill(0)
    .show(false)

  df.na.fill(0,Array("population"))
    .show(false)

  df.na.fill("")
    .show(false)

  df.na.fill("unknown",Array("city"))
    .na.fill("",Array("type"))
    .show(false)

  // Array and map columns
}
