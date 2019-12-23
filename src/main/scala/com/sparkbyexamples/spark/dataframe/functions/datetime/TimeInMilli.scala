package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

object TimeInMilli extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val df = Seq(1).toDF("seq").select(
    current_timestamp().as("current_time"),
    unix_timestamp().as("milliseconds")
    )

  df.printSchema()
  df.show(false)

  //Convert milliseconds to timestamp
  df.select(
    col("milliseconds").cast(TimestampType).as("current_time"),
      col("milliseconds").cast("timestamp").as("current_time2")
  ).show(false)

  //convert timestamp to milliseconds
  df.select(
    unix_timestamp(col("current_time")).as("unix_milliseconds"),
    col("current_time").cast(LongType).as("time_to_milli")
  ).show(false)

}
