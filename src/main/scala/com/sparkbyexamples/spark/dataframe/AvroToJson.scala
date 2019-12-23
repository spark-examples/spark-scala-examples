package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object AvroToJson extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read avro file
  val df = spark.read.format("avro")
    .load("src/main/resources/zipcodes.avro")
  df.show()
  df.printSchema()

  //convert to json
  df.write.mode(SaveMode.Overwrite)
    .json("/tmp/json/zipcodes.json")

}
