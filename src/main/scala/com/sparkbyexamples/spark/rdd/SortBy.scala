package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SortBy {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd:RDD[String] = sc.textFile("C://000_Projects/opt/BigData/zipcodes-noheader.csv")

    val rddZip:RDD[ZipCode] = rdd.map(f=>{
      val arr = split(f)
      ZipCode(arr(0).toInt,arr(1),arr(3),arr(4))
    })

    //SortBy
    val rddSort = rddZip.sortBy(f=>f.recordNumber)
    rddSort.collect().foreach(f=>println(f.toString))

    //SorybyKey
    //First create pairRDD
    val rddTuple=rddZip.map(f=>{
      Tuple2(f.recordNumber,f.toString)
    })
    rddTuple.sortByKey().collect().foreach(f=>println(f._2))
  }

  def split(str:String): Array[String] ={
    str.split(",")
  }

}

