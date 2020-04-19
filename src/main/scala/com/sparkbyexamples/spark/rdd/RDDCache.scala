package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDCache extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  val sc = spark.sparkContext

  val rdd = sc.textFile("src/main/resources/zipcodes-noheader.csv")

  val rdd2:RDD[ZipCode] = rdd.map(row=>{
    val strArray = row.split(",")
    ZipCode(strArray(0).toInt,strArray(1),strArray(3),strArray(4))
  })

  rdd2.cache()


  println(rdd2.count())
}
