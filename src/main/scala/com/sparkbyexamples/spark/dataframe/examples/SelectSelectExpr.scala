package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object SelectSelectExpr extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val df = spark.createDataFrame(data).toDF("language","users_count")
  df.select("language","users_count as count").show() //Example 1
  df.select(df("language"),df("users_count").as("count")).show() //Example 2
  df.select(col("language"),col("users_count")).show() ////Example 3
  //df.select("language",col("users_count")).show() ////Example 3

  df.selectExpr("language","users_count as count").show() //Example 1
  //df.selectExpr(df("language"),df("users_count").as("count")).show() //Example 2
  //df.selectExpr(col("language"),col("users_count")).show() ////Example 3

}
