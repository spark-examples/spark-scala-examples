package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_timestamp, _}

object Spark3Date extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val df3 = Seq(1).toDF("seq").select(
    current_date().as("current_date"),
    current_timestamp().as("current_time"),
    unix_timestamp().as("epoch_time_seconds")
  )
//
//  val data2 = df.collect()
//  data2.foreach(println)
//
//  val df2 = Seq(("06-03-2009","07-01-2009 12:01:19.000")).toDF("Date","Time").select(
//    col("Date"),col("Time"),
//    to_date(col("Date"),"MM-dd-yyyy").as("to_date"),
//    to_timestamp(col("Time"),"MM-dd-yyyy HH:mm:ss.SSS").as("to_timestamp")
//  )
//  df2.show(false)
//
//  val df3 = Seq(("06-03-1500","07-01-1500 12:01:19.000")).toDF("Date","Time").select(
//    col("Date"),col("Time"),
//    to_date(col("Date"),"MM-dd-yyyy").as("to_date"),
//    to_timestamp(col("Time"),"MM-dd-yyyy HH:mm:ss.SSS").as("to_timestamp")
//
//  )
  val df=spark.range(1,10000).toDF("num")
  println("Before re-partition :"+df.rdd.getNumPartitions)
  df.createOrReplaceTempView("RANGE_TABLE")
  val df2=spark.sql("SELECT /*+ REPARTITION(20) */ * FROM RANGE_TABLE")
  println("After re-partition :"+df2.rdd.getNumPartitions)

}
