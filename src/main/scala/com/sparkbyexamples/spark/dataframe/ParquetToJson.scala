package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetToJson extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read parquet file
  val df = spark.read.format("parquet")
    .load("src/main/resources/zipcodes.parquet")
  df.show()
  df.printSchema()

  //convert to json
  df.write.mode(SaveMode.Overwrite)
    .json("/tmp/json/zipcodes.json")

}
