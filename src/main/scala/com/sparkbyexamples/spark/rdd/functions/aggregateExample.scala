package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object aggregateExample {

  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val rdd = spark.sparkContext.parallelize(
    List("Germany","India","USA","USA","London","Russia","Mexico","Brazil","Canada","China")
  )

  val listRdd = spark.sparkContext.parallelize(List(9,2,3,4,5,6,7,8))

  def param1= (accu:Int, v:Int) => accu + v
  def param2= (accu1:Int,accu2:Int) => accu1 + accu2
  println(listRdd.aggregate(3)(param1,param2))


  val inputRDD = spark.sparkContext.parallelize(List(("A", 20),("B", 30),("C", 40)))

  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println(inputRDD.aggregate(0)(param3,param4))
}
