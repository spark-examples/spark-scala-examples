package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToAvroParquetJson extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read csv with options
  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("src/main/resources/zipcodes.csv")
  df.show()
  df.printSchema()

  //convert to avro
  df.write.format("avro").mode(SaveMode.Overwrite)
    .save("/tmp/avro/zipcodes.avro")

  //convert to avro by partition
  df.write.partitionBy("State","Zipcode")
    .format("avro")
    .mode(SaveMode.Overwrite)
    .save("/tmp/avro/zipcodes_partition.avro")

  //convert to parquet
  df.write.mode(SaveMode.Overwrite).parquet("/tmp/parquet/zipcodes.parquet")

  //convert to csv
  df.write.mode(SaveMode.Overwrite).json("/tmp/json/zipcodes.json")
}
