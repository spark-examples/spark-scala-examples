package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object JsonToAvroCsvParquet extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read json file into dataframe
  val df = spark.read.json("src/main/resources/zipcodes.json")
  df.printSchema()
  df.show(false)

  //convert to avro
  df.write.format("avro").save("/tmp/avro/zipcodes.avro")

  //convert to avro by partition
  df.write.partitionBy("State","Zipcode")
    .format("avro").save("/tmp/avro/zipcodes_partition.avro")

  //convert to parquet
  df.write.parquet("/tmp/parquet/zipcodes.parquet")

  //convert to csv
  df.write.option("header","true").csv("/tmp/csv/zipcodes.csv")
}
