package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, last_day, to_date}

object DateLastDay extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  Seq(("2019-01-01"),("2020-02-24"),("2019-02-24"),
      ("2019-05-01"),("2018-03-24"),("2007-12-19"))
    .toDF("Date").select(
    col("Date"),
    last_day(col("Date")).as("last_day")
  ).show()


  Seq(("06-03-2009"),("07-24-2009")).toDF("Date").select(
    col("Date"),
    last_day(to_date(col("Date"),"MM-dd-yyyy")).as("last_day")
  ).show()

}
