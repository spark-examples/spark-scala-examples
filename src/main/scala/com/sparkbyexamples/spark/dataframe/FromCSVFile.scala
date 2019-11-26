package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object FromCSVFile {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext

    val filePath="src/main/resources/zipcodes.csv"

    //Chaining multiple options
    val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath)
    df2.show(false)
    df2.printSchema()

    df2.write.json("c:/tmp/spark_output/zipcodes")

  }
}
