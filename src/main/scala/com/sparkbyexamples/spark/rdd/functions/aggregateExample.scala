package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object aggregateExample extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local[3]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //aggregate example
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("output 1 : "+listRdd.aggregate(0)(param0,param1))


  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("output 2 : "+inputRDD.aggregate(0)(param3,param4))

  println("Number fo Partitions :"+listRdd.getNumPartitions)
  //aggregate example
  println("output 1 : "+listRdd.aggregate(1)(param0,param1))

}
