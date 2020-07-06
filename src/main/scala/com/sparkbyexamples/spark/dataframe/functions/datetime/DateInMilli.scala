package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{unix_timestamp, _}
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
    unix_timestamp().as("unix_timestamp_seconds")
  )

  df.printSchema()
  df.show(false)

  //Convert unix seconds to date
  df.select(
    to_date(col("unix_timestamp_seconds").cast(TimestampType)).as("current_date")
  ).show(false)

  //convert date to unix seconds
  df.select(
    unix_timestamp(col("current_date")).as("unix_seconds"),
    unix_timestamp(lit("12-21-2019"),"mm-DD-yyyy").as("unix_seconds2")
  ).show(false)

}