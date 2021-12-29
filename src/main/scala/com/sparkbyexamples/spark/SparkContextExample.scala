package com.sparkbyexamples.spark



import java.util

import com.sparkbyexamples.spark.dataframe.functions.SortExample.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{lit, max, min}


object SparkContextExample extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExajjmples.com")
    .getOrCreate();


  val dfa = spark.read.csv("src/main/resources/data.csv").toDF("x","y")
  dfa.na.drop().show()
  val df=dfa.na.drop()

  //FINDING MIN MAX FOR EACH COLUMN SO WE CAN USE IT FOR Min-max normalization
  //for x
  val min_maxX = df.agg(min("x"), max("x")).head()
  val col_minX = min_maxX.getString(0)
  val col_maxX = min_maxX.getString(1)
  println(col_maxX,col_minX)
  //for y
  val min_maxY = df.agg(min("y"), max("y")).head()
  val col_minY = min_maxY.getString(0)
  val col_maxY = min_maxY.getString(1)
  println(col_maxY,col_minY)

  for (row <- df.rdd.collect)
  {
    val x = row.mkString(",").split(",")(0)
    val y = row.mkString(",").split(",")(1)
    val newX= (x.toDouble-col_minX.toDouble)/(col_maxX.toDouble-col_minX.toDouble) //min max normalization type
    val newY= (y.toDouble-col_minY.toDouble)/(col_maxY.toDouble-col_minY.toDouble)
    
}


}
