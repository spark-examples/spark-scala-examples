package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession

object FromJsonFile {

  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("src/main/resources/zipcodes.json")
    //Todo : convert RDD to DataFrame
    rdd.collect().foreach(println)

  }
}
