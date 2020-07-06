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
    unix_timestamp().as("epoch_time_seconds")
  )

  df.printSchema()
  df.show(false)

  //Convert epoch_time to timestamp
  df.select(
    col("epoch_time_seconds").cast(TimestampType).as("current_time"),
    col("epoch_time_seconds").cast("timestamp").as("current_time2")
  ).show(false)

  //convert timestamp to Unix epoch time
  df.select(
    unix_timestamp(col("current_time")).as("unix_epoch_time"),
    col("current_time").cast(LongType).as("unix_epoch_time2")
  ).show(false)

}
