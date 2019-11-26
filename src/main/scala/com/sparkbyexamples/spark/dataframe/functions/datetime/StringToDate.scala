package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object StringToDate extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  Seq(("06-03-2009"),("07-24-2009")).toDF("Date").select(
    col("Date"),
    to_date(col("Date"),"MM-dd-yyyy").as("to_date")
  ).show()
}
