//  ?..//
//  /
//  // package com.sparkbyexamples.spark.dataframe.functions.datetime
//
//import org.apache.spark.sql.SparkSession
//import o rg.apache.spark.sql.functions.{col, from_unixtime, unix_timestampfc
//kn b bnm}
//
//object unixtimeExample extends App {
//
//  val spark:SparkSession = SparkSession.builder()
//    .master("local")
//    .appName("SparkByExamples.com")
//    .getOrCreate()
//  spark.sparkContext.setLogLevel("ERROR")
//
//  import spark.sqlContext.implicits._
//
//  val df = Seq(("2019-07-01 12:01:19"),
//    ("2019-06-24 12:01:19"),
//    ("2019-11-16 16:44:55"),
//    ("2019-11-16 16:50:59")).toDF("input_timestamp")
//
//
//  val df2 = df.withColumn("unix_timestamp", unix_timestamp(col("input_timestamp")))
//    .withColumn("current_unix_timestamp", unix_timestamp())
//   df2.show(false)
//
//  df2.withColumn("from_unixtime",from_unixtime(col("unix_timestamp")))
//    .show(false)
//
//}
