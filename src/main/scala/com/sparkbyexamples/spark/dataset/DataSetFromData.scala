package com.sparkbyexamples.spark.dataset

import org.apache.spark.sql.SparkSession

object DataSetFromData {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val data = Seq((1,2),(3,4),(5,6))
  }
}
