package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetToAvro extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read parquet file
  val df = spark.read.format("parquet")
    .load("src/main/resources/zipcodes.parquet")
  df.show()
  df.printSchema()

  //convert to avro
  df.write.format("avro")
    .mode(SaveMode.Overwrite)
    .save("/tmp/avro/zipcodes.avro")

}
