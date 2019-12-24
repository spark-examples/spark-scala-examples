package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}

object DateInMilli extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val df = Seq(1).toDF("seq").select(
    current_date().as("current_date"),
    unix_timestamp().as("milliseconds")
    )

  df.printSchema()
  df.show(false)

  //Convert milliseconds to date
  df.select(
    to_date(col("milliseconds").cast(TimestampType)).as("current_date")
  ).show(false)

  //convert date to milliseconds
  df.select(
    unix_timestamp(col("current_date")).as("unix_milliseconds")
  ).show(false)

}
