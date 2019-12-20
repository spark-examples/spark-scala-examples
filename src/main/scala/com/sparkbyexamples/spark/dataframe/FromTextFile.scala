package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object FromTextFile {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    //returns DataFrame
    val df = spark.read.text("src/main/resources/txt")
    df.printSchema()
    df.show(false)

    // returns Dataset[String]
    val ds = spark.read.textFile("src/main/resources/txt")
    ds.printSchema()
    ds.show(false)
  }
}
