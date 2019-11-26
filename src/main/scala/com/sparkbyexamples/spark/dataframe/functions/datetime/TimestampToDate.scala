package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import org.apache.spark.sql.types.DateType

object TimestampToDate extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val df = Seq(("2019-07-01 12:01:19.000"),
    ("2019-06-24 12:01:19.000"),
    ("2019-11-16 16:44:55.406"),
    ("2019-11-16 16:50:59.406")).toDF("input_timestamp")

  //Timestamp String to DateType
  df.withColumn("datetype",
    to_date(col("input_timestamp"),"yyyy-MM-dd"))
    .show(false)

  //Timestamp type to DateType
  df.withColumn("ts",to_timestamp(col("input_timestamp")))
    .withColumn("datetype",to_date(col("ts")))
    .show(false)

  //Using Cast
  df.withColumn("ts",to_timestamp(col("input_timestamp")))
    .withColumn("datetype",col("ts").cast(DateType))
    .show(false)
}
