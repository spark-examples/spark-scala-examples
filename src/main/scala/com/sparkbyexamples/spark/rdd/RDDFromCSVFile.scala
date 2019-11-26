package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDFromCSVFile {

  def main(args:Array[String]): Unit ={

    def splitString(row:String):Array[String]={
      row.split(",")
    }

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("src/main/resources/zipcodes-noheader.csv")

    val rdd2:RDD[ZipCode] = rdd.map(row=>{
     val strArray = splitString(row)
      ZipCode(strArray(0).toInt,strArray(1),strArray(3),strArray(4))
    })

    rdd2.foreach(a=>println(a.city))
  }

}


