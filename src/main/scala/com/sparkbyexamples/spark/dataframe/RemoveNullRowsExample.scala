package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object RemoveNullRowsExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val filePath="src/main/resources/small_zipcode.csv"

  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(filePath)
  df.printSchema()
  df.show(false)

  df.na.drop()
    .show(false)

  // Array and map columns
}
