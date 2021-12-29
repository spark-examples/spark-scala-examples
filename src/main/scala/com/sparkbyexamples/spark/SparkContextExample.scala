package com.sparkbyexamples.spark

import com.sparkbyexamples.spark.dataframe.functions.SortExample.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{min, max, lit}

object SparkContextExample extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate();

  spark.sparkContext.setLogLevel("ERROR")


  val sparkContext:SparkContext = spark.sparkContext
  val sqlCon:SQLContext = spark.sqlContext

  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

  /*println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);*/

  val df = spark.read.csv("src/main/resources/data.csv").toDF("x","y")
  df.show()
  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample-test")
    .getOrCreate();

 val (xMin, xMax) = df.agg(min("y"), max("y")).first match {
    case Row(x: Double, y: Double) => (x, y)
  }

  val scaledRange = lit(1 )// Range of the scaled variable
  val scaledMin = lit(0)  // Min value of the scaled variable
  val yNormalized = (df.col("y") - xMin) / (xMax - xMin) // v normalized to (0, 1) range

  val yScaled = scaledRange * yNormalized + scaledMin

  df.withColumn("yScaled", yScaled).show


  /*println("Second SparkContext:")
  println("APP Name :"+sparkSession2.sparkContext.appName);
  println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
  println("Master :"+sparkSession2.sparkContext.master);*/


}
