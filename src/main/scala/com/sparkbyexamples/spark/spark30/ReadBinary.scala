package com.sparkbyexamples.spark.spark30

import org.apache.spark.sql.SparkSession

object ReadBinary extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.format("binaryFile").load("C:\\tmp\\binary\\spark.png")
  df.printSchema()
  df.show()

  val df2 = spark.read.format("binaryFile").load("C:\\tmp\\binary\\")
  df2.printSchema()
  //df2.show(false)

  val df3 = spark.read.format("binaryFile").load("C:\\tmp\\binary\\*.png")
  df3.printSchema()
  df3.show(false)

  // To load files with paths matching a given glob pattern while keeping the behavior of partition discovery
  val df4 = spark.read.format("binaryFile")
       .option("pathGlobFilter", "*.png")
       .load("C:\\tmp\\binary\\")
  df4.printSchema()
  //df4.show(false)
}
