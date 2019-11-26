package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object TimestampDiff extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  //Difference between two timestamps
  val df = Seq(("2019-07-01 12:01:19.000"),
    ("2019-06-24 12:01:19.000"),
    ("2019-11-16 16:44:55.406"),
    ("2019-11-16 16:50:59.406")).toDF("input_timestamp")

  df.withColumn("input_timestamp",
        to_timestamp(col("input_timestamp")))
      .withColumn("current_timestamp",
        current_timestamp().as("current_timestamp"))
      .withColumn("DiffInSeconds",
        current_timestamp().cast(LongType) - col("input_timestamp").cast(LongType))
      .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))
      .withColumn("DiffInHours",round(col("DiffInSeconds")/3600))
      .withColumn("DiffInDays",round(col("DiffInSeconds")/24*3600)
  ).show(false)

  //Difference between two timestamps when input has just timestamp
  val df1 = Seq(("12:01:19.000","13:01:19.000"),
    ("12:01:19.000","12:02:19.000"),
    ("16:44:55.406","17:44:55.406"),
    ("16:50:59.406","16:44:59.406"))
    .toDF("from_timestamp","to_timestamp")

  df1.withColumn("from_timestamp",
    to_timestamp(col("from_timestamp"),"HH:mm:ss.SSS"))
    .withColumn("to_timestamp",
      to_timestamp(col("to_timestamp"),"HH:mm:ss.SSS"))
    .withColumn("DiffInSeconds",
      col("from_timestamp").cast(LongType) - col("to_timestamp").cast(LongType))
    .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))
    .withColumn("DiffInHours",round(col("DiffInSeconds")/3600))
    .show(false)

  //Difference between two dates when dates are not in Spark DateType format 'yyyy-MM-dd  HH:mm:ss.SSS'.
  //Note that when dates are not in Spark DateType format, all Spark functions returns null
  //Hence, first convert the input dates to Spark DateType using to_timestamp function
  val dfDate = Seq(("07-01-2019 12:01:19.406"),
    ("06-24-2019 12:01:19.406"),
    ("11-16-2019 16:44:55.406"),
    ("11-16-2019 16:50:59.406")).toDF("input_timestamp")

  dfDate.withColumn("input_timestamp",
          to_timestamp(col("input_timestamp"),"MM-dd-yyyy HH:mm:ss.SSS"))
    .withColumn("current_timestamp",
          current_timestamp().as("current_timestamp"))
    .withColumn("DiffInSeconds",
          current_timestamp().cast(LongType) - col("input_timestamp").cast(LongType))
    .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))
    .withColumn("DiffInHours",round(col("DiffInSeconds")/3600))
    .withColumn("DiffInDays",round(col("DiffInSeconds")/24*3600))
    .show(false)


}
