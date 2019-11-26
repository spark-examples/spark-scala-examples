package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,hour,minute,second}

object GetTimeFromTimestamp extends App {

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


  df.withColumn("hour", hour(col("input_timestamp")))
    .withColumn("minute", minute(col("input_timestamp")))
    .withColumn("second", second(col("input_timestamp")))
    .show(false)

}
